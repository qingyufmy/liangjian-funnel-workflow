import {ROLES,STATE_LABELS} from './roles.js';
import {translateReasonMessage} from './copy/reasons.js';
const $=id=>document.getElementById(id);
const make=(tag,value,cls)=>{const e=document.createElement(tag);if(value!=null)e.textContent=value;if(cls)e.className=cls;return e;};
const n=v=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('zh-CN',{maximumFractionDigits:2}):'—';
export function studioFacts(state){
  const active=state.plans?.filter(x=>x.status==='ACTIVE_TODAY'||x.status==='ACTIVE').length??null;
  const invalid=state.plans?.filter(x=>x.status==='INVALIDATED').length??null;
  const callRows=state.tasks.filter(row=>row.provenance?.table==='model_calls'&&row.provenance?.id&&row.provenance?.sha256);
  const tasks=callRows.length?callRows.length:null;
  const knownPositions=state.positions!==null;
  return {active,invalid,tasks,title:state.risk?.latch_record?'交易团队处于只记录保护':active!==null?`跟踪 ${active} 条有效计划`:'看清每一步，再作判断',subtitle:active!==null?`已载入 ${state.plans.length} 条计划，${invalid} 条作废。${knownPositions?`账本 ${state.positions.length} 条持仓记录。`:'持仓账本待读取。'}未读到的环节不作判断。`:'计划、条件与模拟账户，各自回到原始记录。'};
}
export function mountStudio(){
  const main=make('div',null,'studio-main'),room=document.querySelector('.room'),feed=document.querySelector('.plan-record-grid .feed');
  room.before(main);main.append(room,feed);
  $('workstations').replaceChildren(...ROLES.map(r=>{const a=make('article',null,'workstation');a.dataset.id=r.id;a.style.setProperty('--role-color',r.color);const screen=make('div','等待工作原件','station-screen');const avatar=make('div',r.ch,'station-avatar');avatar.setAttribute('aria-hidden','true');a.append(screen,avatar,make('strong',r.short==='组合'?'组合经理':r.short,'station-name'),make('span','尚无状态记录','station-status'));return a;}));
  const stats=[['模拟盘净值','nav'],['仓位','position'],['今日信号','signal'],['形态确认','confirmation'],['系统健康','health'],['模型调用','calls']];
  $('studio-stats').replaceChildren(...stats.map(([label,key])=>{const a=make('article',null,'stat-card');a.dataset.stat=key;a.append(make('span',label,'stat-label'),make('strong','—','stat-value'),make('div','走势待有序原件','stat-history'),make('small','等待来源'));return a;}));
}
export function renderStudio(state){
  const f=studioFacts(state);$('today-title').textContent=f.title;$('today-subtitle').textContent=f.subtitle;
  $('today-evidence').textContent=state.day?`${state.day} · ${state.hhmm??'时刻待提供'} · 只读观测`:'等待有来源的交易记录';
  const vals={nav:[n(state.account?.nav),state.account?.nav!=null?'账本来源见观测证据':'净值账本未提供'],position:['—','未收到完整持仓估值'],signal:['—','完整信号流水待接入'],confirmation:[state.conds?`${state.conds.met.filter(Boolean).length} / ${state.conds.met.length}`:'—',state.conds?'当前焦点条件，不是全日统计':'逐项条件待原件'],health:[state.risk?.latch_record?'保护中':'待核验','不以页面连通代替业务验收'],calls:[n(f.tasks),f.tasks?'已载入调用记录，非全日总计':'调用记录待接入']};
  for(const [key,[value,note]]of Object.entries(vals)){const el=document.querySelector(`[data-stat="${key}"]`);el.querySelector('.stat-value').textContent=value;el.querySelector('small').textContent=note;}
  for(const r of ROLES){const el=$('workstations').querySelector(`[data-id="${r.id}"]`),s=state.roles[r.id],known=s.label!=='缺少工作事件';el.dataset.active=String(known&&['work','talk'].includes(s.state));el.querySelector('.station-status').textContent=known?(STATE_LABELS[s.state]??'状态待核验'):'尚无状态记录';el.querySelector('.station-screen').textContent=known?translateReasonMessage(s.label===s.state?STATE_LABELS[s.state]:s.label):'等待工作原件';}
  const active=ROLES.filter(r=>['work','talk'].includes(state.roles[r.id].state));$('crew-summary').textContent=active.length?`${active.map(x=>x.short).join('、')}工作中 · 依据角色事件`:'六人接力 · 未见正在工作原件';
  const risk=state.risk,limits=[['今日新开',risk?.open,risk?.max_open],['同簇占用',risk?.cluster_open,risk?.max_cluster],['现金保留',risk?.cash_pct,100],['风险占用',risk?.risk_pct,100]];
  $('limit-bars').replaceChildren(...limits.map(([label,value,max])=>{const row=make('div',null,'limit-line'),head=make('div');head.append(make('span',label),make('span',value==null?'待原件':`${n(value)} / ${n(max)}`));const track=make('div',null,'limit-track');track.setAttribute('aria-hidden','true');if(value!=null&&max>0){const fill=make('span');fill.style.width=`${Math.min(100,Math.max(0,value/max*100))}%`;track.append(fill);}row.append(head,track);return row;}));
}
