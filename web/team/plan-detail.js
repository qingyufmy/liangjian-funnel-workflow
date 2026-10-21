import {conditionLabel,planStatusLabel,strategyLabel} from './labels.js';
import {reasonText} from './copy/reasons.js';
const num=v=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('zh-CN',{maximumFractionDigits:4}):'未提供';
const el=(tag,content,cls)=>{const n=document.createElement(tag);if(content!=null)n.textContent=content;if(cls)n.className=cls;return n;};
export function chartModel(plan){
  const bars=(plan.bars??[]).filter(b=>typeof b.close==='number'&&Number.isFinite(b.close)&&b.closed===true).slice().sort((a,b)=>a.at.localeCompare(b.at));
  const zone=Array.isArray(plan.zone_values)?plan.zone_values:[];
  const levels=[...bars.map(x=>x.close),...zone,plan.stop].filter(v=>typeof v==='number'&&Number.isFinite(v));
  if(!bars.length||!levels.length)return null;
  const min=Math.min(...levels),max=Math.max(...levels),range=max-min||1;
  const first=Date.parse(bars[0].at),last=Date.parse(bars.at(-1).at),span=last-first||300000;
  const x=b=>8+(Date.parse(b.at)-first)/span*304,y=v=>8+(max-v)/range*110;
  const segments=[];let current=[];
  for(const b of bars){const prev=current.at(-1);if(prev&&Date.parse(b.at)-Date.parse(prev.at)!==300000){segments.push(current);current=[];}current.push(b);}if(current.length)segments.push(current);
  return {bars,segments:segments.map(s=>s.map(b=>`${x(b)},${y(b.close)}`).join(' ')),points:bars.map(b=>({x:x(b),y:y(b.close),bar:b})),zone:zone.length===2?{y:y(zone[1]),h:Math.max(.5,y(zone[0])-y(zone[1]))}:null,stopY:typeof plan.stop==='number'?y(plan.stop):null,maxVolume:Math.max(0,...bars.map(b=>typeof b.volume==='number'?b.volume:0)),coverage:{first:bars[0].at,last:bars.at(-1).at,count:bars.length,gapped:segments.length>1}};
}
function svgEl(tag,attrs){const n=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const[k,v]of Object.entries(attrs))n.setAttribute(k,String(v));return n;}
export function planChart(plan,compact=false){
  const model=chartModel(plan);if(!model)return el('span','5分钟原件待接入','chart-missing');
  const svg=svgEl('svg',{viewBox:'0 0 320 150',role:'img','aria-label':`${model.coverage.count}根已闭合5分钟价格观测；仅${model.coverage.first.slice(11,16)}至${model.coverage.last.slice(11,16)}局部，不补线`,class:compact?'plan-spark':'plan-chart'});
  if(model.zone)svg.append(svgEl('rect',{x:0,y:model.zone.y,width:320,height:model.zone.h,fill:'#f59e0b','fill-opacity':.14}));
  if(model.stopY!==null)svg.append(svgEl('line',{x1:0,x2:320,y1:model.stopY,y2:model.stopY,stroke:'#f87171','stroke-width':1,'stroke-dasharray':'4 3'}));
  for(const p of model.points){if(p.bar.volume!=null&&model.maxVolume>0)svg.append(svgEl('rect',{x:p.x-2,y:146-p.bar.volume/model.maxVolume*20,width:4,height:p.bar.volume/model.maxVolume*20,fill:'#71717a','fill-opacity':.4}));const dot=svgEl('circle',{cx:p.x,cy:p.y,r:1.7,fill:'#e4e4e7'});dot.append(svgEl('title',{}));dot.firstChild.textContent=`${p.bar.at} 收盘${p.bar.close}；SHA ${p.bar.sha256}`;svg.append(dot);}
  for(const points of model.segments)svg.append(svgEl('polyline',{points,fill:'none',stroke:'#e4e4e7','stroke-width':1.8}));
  return svg;
}
export function openPlanDetail(plan){
  let dialog=document.getElementById('plan-drawer');if(!dialog){dialog=el('dialog',null,'plan-drawer');dialog.id='plan-drawer';dialog.setAttribute('aria-labelledby','plan-drawer-title');document.body.append(dialog);}
  const title=el('h2',`${plan.name??plan.code} · ${plan.code}`);title.id='plan-drawer-title';
  const close=el('button','关闭');close.type='button';close.onclick=()=>dialog.close();const head=el('div',null,'drawer-head');head.append(title,close);
  const stats=el('dl',null,'drawer-stats');for(const[label,value]of [['计划区间',(plan.zone_values??[]).map(num).join('–')||plan.zone||'未提供'],['止损',num(plan.stop)],['目标',num(plan.target)],['盈亏比',num(plan.rr)]]){const group=el('div');group.append(el('dt',label),el('dd',value));stats.append(group);}
  const content=el('div',null,'drawer-content');content.append(el('p',`${strategyLabel(plan.strat)} · ${planStatusLabel(plan.status)}`),stats,el('h3','闭合5分钟走势'),planChart(plan),el('p','价格线：浅灰白 · 计划区间：浅金 · 止损：红色虚线 · 成交量：灰柱。仅绘已采到的闭合原件；时间缺口不连线，不代表全天完整覆盖。','chart-legend'),el('h3','形态条件'));
  const conditions=el('ul',null,'drawer-conditions');
  for(const c of plan.conditions??[]){const item=el('li');item.append(el('strong',conditionLabel(c.code)),el('span',({MET:'满足',UNMET:'未满足',UNPROVEN:'证据不足'})[c.status]??'证据不足'));const fact=el('p',`阈值 ${JSON.stringify(c.threshold??'未提供')}；观测 ${JSON.stringify(c.value??'未提供')}；${c.at??'时刻未提供'}`);item.append(fact);conditions.append(item);}
  if(!conditions.children.length)conditions.append(el('li','未接入该计划的逐项原件，不能由价格或状态推断形态已满足。'));content.append(conditions);
  const raw=el('details');raw.append(el('summary','原始原因码与来源'),el('pre',JSON.stringify({plan_id:plan.source_id,sha256:plan.source_sha256,updated_at:plan.source_at,reason_codes:plan.reason_codes??[],conditions:(plan.conditions??[]).map(c=>({code:c.code,explanation:reasonText(c.code),source_id:c.source_id,sha256:c.sha256}))},null,2)));content.append(raw);dialog.replaceChildren(head,content);dialog.showModal();
}
