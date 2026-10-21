import {ROLES,STATE_LABELS} from './roles.js';
import {emptyState,reduceEvent,addedPositions} from './model.js';
import {createStage} from './stage.js';
import {conditionLabel,strategyLabel,planStatusLabel,clusterLabel} from './labels.js';

const $=id=>document.getElementById(id);
const number=(v,digits=2)=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('zh-CN',{minimumFractionDigits:digits,maximumFractionDigits:digits}):'—';
const text=(node,value)=>{node.textContent=value==null?'—':String(value);};
const role=id=>ROLES.find(r=>r.id===id);
const taskStatus=value=>({VALIDATED:'校验通过',VALIDATION_FAILED:'校验未通过',ENGINE_TRACKING_NO_REVIEW_CANDIDATE:'引擎跟踪，没有需模型评审的候选'})[value]??'任务状态待说明';
const byText=(tag,value,className)=>{const el=document.createElement(tag);text(el,value);if(className)el.className=className;return el;};
let state=emptyState(),scene=null,reviewScene=null,demoTimer=null,sse=null,replay=null,replayIndex=0,handoffTimer=null;
function fillRoles(root){
  root.replaceChildren(...ROLES.map(r=>{const item=document.createElement('article');item.dataset.role=r.id;item.append(byText('strong',r.name),byText('span',r.cues.join(' · ')),byText('span','缺少工作事件','role-state'));return item;}));
}
fillRoles($('role-fallback'));fillRoles($('review-fallback'));
$('role-picks').replaceChildren(...ROLES.map(r=>{const b=byText('button',r.short);b.type='button';b.setAttribute('aria-label',`近看${r.name}`);b.onclick=()=>scene?.focus(r.id);return b;}));
$('overview').onclick=()=>scene?.focus('all');
for(const r of ROLES){const option=byText('option',r.name);option.value=r.id;$('preview-role').append(option);}
$('role-notes').replaceChildren(...ROLES.map(r=>{const article=document.createElement('article');article.append(byText('b',r.name),byText('p',r.cues[0]),byText('p',r.cues[1]),byText('p',`work：${r.work}`));return article;}));
function table(host,columns,rows){
  if(!rows.length){host.replaceChildren(byText('p','已读取到空列表；不是缺源。','empty'));return;}
  const table=document.createElement('table'),head=document.createElement('thead'),tr=document.createElement('tr');
  for(const [,label]of columns)tr.append(byText('th',label));head.append(tr);table.append(head);
  const body=document.createElement('tbody');
  for(const row of rows){const line=document.createElement('tr');for(const [key]of columns)line.append(byText('td',key==='strat'?strategyLabel(row[key]):key==='status'?planStatusLabel(row[key]):key==='cluster'?clusterLabel(row[key]):row[key]));const cell=document.createElement('td'),details=document.createElement('details');details.append(byText('summary','来源'),byText('p',`原计划：${row.source_id??'缺少记录ID'} · SHA ${row.source_sha256??'缺失'}`),byText('p',`原策略 ${row.strat??'缺源'}；原状态 ${row.status??'缺源'}；原分组 ${row.cluster??'缺源'}`));if(row.status_source_id)details.append(byText('p',`状态更新：${row.status_source_id} · ${row.status_source_at} · SHA ${row.status_source_sha256}`));cell.append(details);line.append(cell);body.append(line);}
  tr.append(byText('th','证据'));
  table.append(body);host.replaceChildren(table);
}
function render(){
  text($('day-clock'),state.day?`交易日 ${state.day} · ${state.hhmm??'—'}`:'等待带来源的时钟事件');
  if(state.account){
    for(const [field,id]of[['nav','nav'],['cash','cash'],['float','float']])text($(`account-${id}`),number(state.account[field]));
    for(const field of['day','week']){const value=state.account[`${field}_pct`],el=$(`account-${field}`);text(el,value==null?'—':`${value>=0?'+':''}${number(value)}%`);el.className=value==null?'':value>=0?'up':'down';}
    const p=state.account.provenance;text($('account-source'),p?.id&&p?.sha256?`观测 ${state.account.at??'时刻缺失'} · 来源 ${p.id} · SHA ${p.sha256}`:'已收到事件，来源记录未提供');
  }
  text($('account-counts'),`${state.plans===null?'—':state.plans.filter(r=>r.status!=='INVALIDATED').length} / — / —`);
  for(const r of ROLES){const current=state.roles[r.id];const article=$('role-fallback').querySelector(`[data-role="${r.id}"]`);text(article.querySelector('.role-state'),current.label);}
  const working=ROLES.filter(r=>['work','talk'].includes(state.roles[r.id].state));
  text($('room-current'),working.length?working.map(r=>r.name).join('、'):state.events?'已读记录中未见正在工作状态':'工作状态未接入');
  const nextTask=state.tasks.find(t=>['queued','running','PENDING','RUNNING','now'].includes(t.status));
  text($('room-next'),nextTask?.label??'调度记录未接入');
  text($('room-attention'),state.risk?.latch_record?'只记录保护锁已触发，请查看原件':state.alerts.length?'有需关注的记录，请查看下方详情':'异常来源未接入，暂不能判断');
  if(state.tasks.length){text($('task-count'),`${state.tasks.length} 条记录，列表内滚动查看全部`);$('tasks').replaceChildren(...state.tasks.slice().reverse().map(t=>{const li=document.createElement('li'),details=document.createElement('details');li.append(byText('time',t.at??t.time??'—'),byText('b',`${t.label==='Team A4'?'团队交易员':t.label??'任务'}：${taskStatus(t.status)}`),byText('small',t.note??'无附加说明'));details.append(byText('summary','原状态与来源'),byText('small',`${t.status}；${t.reason_codes?.join(', ')??''}；${t.provenance?.id??'记录缺源'}；SHA ${t.provenance?.sha256??'缺源'}`));li.append(details);return li;}));}
  if(state.focus){const f=state.focus;$('focus').replaceChildren(byText('h3',`${f.name??''} ${f.code??'标的缺源'}`),byText('p',`${strategyLabel(f.strat)}，现价 ${number(f.price)}。条件按本条原观测核对。`));text($('focus-clock'),f.at??'未提供观测时刻');}
  if(state.conds)$('conditions').replaceChildren(...state.conds.met.map((value,i)=>{const line=document.createElement('div');line.className='condition';line.append(byText('b',value===null?'?':value?'✓':'○'),byText('span',state.conds.labels?.[i]?conditionLabel(state.conds.labels[i]):`条件 ${i+1}（名称待来源）`,'condition-name'),byText('b',value===null?'证据不足':value?'满足':'未满足'));return line;}));
  text($('raw-contracts'),JSON.stringify({strategy:state.focus?.strat,condition_keys:state.conds?.labels,risk_reason:state.risk?.reason_code,risk_mode:state.risk?.latch_record?'record':undefined},null,2));
  if(state.risk){const r=state.risk;text($('risk'),`新开 ${r.open??'—'} / ${r.max_open??'—'}，风险 ${r.risk_pct??'—'}%，现金 ${r.cash_pct??'—'}%，同簇 ${r.cluster??'—'}。${r.latch_record?'只记录保护锁':r.blocked?'风控拦截':'未报告拦截，不代表执行链已验证'}`);}
  if(state.feed.length){text($('feed-count'),`${state.feed.length} 条`);$('feed').replaceChildren(...state.feed.slice(-25).reverse().map(f=>{const li=document.createElement('li');li.append(byText('b',role(f.id).name),byText('time',f.at??'未提供时刻'),byText('p',f.text));return li;}));}
  if(state.alerts.length){$('needs-attention').replaceChildren(byText('h2','需要你看'),...state.alerts.slice(-5).map(a=>{const box=document.createElement('div'),details=document.createElement('details');box.append(byText('p',`${a.at??'时刻缺源'}，${a.who??'来源缺失'}：${a.text==='TEAM_MONITOR_DEGRADED'?'盘中资料异常，账户已切入只记录保护；原因为历史记录，不代表当前状态。':a.text??'说明缺失'}`));details.append(byText('summary','查看原件说明'),byText('p',a.text));box.append(details);return box;}));}
  if(state.plans!==null){const rows=state.plans.filter(r=>r.status!=='INVALIDATED');text($('plan-count'),`${rows.length} 份 · 排除作废 ${state.plans.length-rows.length}`);table($('plans'),[['code','代码'],['name','标的'],['strat','策略'],['cluster','分组'],['zone','触发区'],['stop','止损'],['status','状态']],rows);}
  if(state.positions!==null){text($('position-count'),`账本记录 ${state.positions.length}`);table($('positions'),[['code','代码'],['name','标的'],['qty','数量'],['entry','入场'],['price','现价'],['stop','止损'],['src','来源']],state.positions);}
  text($('event-status'),`已接收 ${state.events} 条事件 · 无写入操作`);
}
export function applyEvent(ev){
  const previousPositions=state.positions;
  state=reduceEvent(state,ev);
  if(ev.type==='role'&&!(ev.id==='tr'&&ev.state==='happy'))scene?.setRole(ev.id,ev.state);
  if(ev.type==='positions'){
    if(addedPositions(previousPositions,ev.rows).length)scene?.setRole('tr','happy');
  }
  if(ev.type==='say')bubble(ev.id,ev.text);
  if(ev.type==='relay'){
    const line=$('handoff-line');text(line,`${role(ev.from).name}把工作交给${role(ev.to).name}；${ev.at??'时刻待来源'}`);
    clearTimeout(handoffTimer);line.classList.remove('is-relaying');
    requestAnimationFrame(()=>{line.classList.add('is-relaying');handoffTimer=setTimeout(()=>line.classList.remove('is-relaying'),700);});
  }
  render();return state.events;
}
export function setRole(id,value,label){applyEvent({type:'role',id,state:value,label});}
export function bubble(id,message){
  const article=$('role-fallback').querySelector(`[data-role="${id}"]`);
  if(article)text(article.querySelector('.role-state'),message);
}
function stopDemo(){
  clearInterval(demoTimer);demoTimer=null;$('demo-toggle').setAttribute('aria-pressed','false');text($('demo-toggle'),'演示：关闭');
  text($('mode-notice'),replay?'10-20 历史局部原件回放，不是当前运行状态。缺失数据仍为「—」。':'设计样板 / 非生产。默认不播放演示、不请求生产数据。缺少来源的数字显示为「—」。');
  for(const r of ROLES)scene?.setRole(r.id,state.roles[r.id].state);
}
$('demo-toggle').onclick=()=>{
  if(replay)return;
  if(demoTimer){stopDemo();return;}
  text($('mode-notice'),'角色动作演示 / 非生产：仅切换动作，不生成计划、信号、成交、发言或账户数字。');
  text($('demo-toggle'),'演示：角色动作');$('demo-toggle').setAttribute('aria-pressed','true');let index=0;
  demoTimer=setInterval(()=>{for(const r of ROLES)scene?.setRole(r.id,'idle');scene?.setRole(ROLES[index++%ROLES.length].id,'work');},1800);
};
$('theme-toggle').onclick=()=>{const dark=document.documentElement.dataset.theme!=='dark';document.documentElement.dataset.theme=dark?'dark':'light';text($('theme-toggle'),dark?'浅色':'深色');};
let reviewPaused=true;
function updatePreview(){
  reviewScene?.setView($('preview-mode').value);$('review-scene').parentElement.dataset.view=$('preview-mode').value;
  reviewScene?.focus($('preview-role').value);reviewScene?.setAll($('preview-state').value);
  for(const item of $('review-fallback').querySelectorAll('.role-state'))text(item,`设计状态：${$('preview-state').value}`);
}
for(const id of['preview-mode','preview-state','preview-role'])$(id).onchange=updatePreview;
$('preview-pause').onclick=()=>{reviewPaused=!reviewPaused;reviewScene?.pause(reviewPaused);$('preview-pause').setAttribute('aria-pressed',String(reviewPaused));text($('preview-pause'),reviewPaused?'动作已暂停':'暂停动作');};
function mountStage(review=false){
  const host=$(review?'review-scene':'scene'),fallback=$(review?'review-fallback':'role-fallback');
  const value=createStage(host,{preview:review,onUnavailable:()=>{host.hidden=true;fallback.classList.add('is-primary');}});
  if(review){reviewScene=value;updatePreview();}else scene=value;
}
function route(){
  const path=location.pathname.replace(/\/$/,'');const review=path==='/team/roles-preview',home=path==='/team';
  $('home-view').hidden=!home;$('preview-view').hidden=!review;$('placeholder-view').hidden=home||review;
  const key=path.split('/')[2]||'home';for(const link of document.querySelectorAll('[data-route]')){if(link.dataset.route===key)link.setAttribute('aria-current','page');else link.removeAttribute('aria-current');}
  if(!home&&!review)text($('placeholder-title'),({roles:'团队与角色',plans:'计划',review:'复盘',settings:'设置'})[key]??'页面尚未开放');
  if(review&&!reviewScene)mountStage(true);if(home&&!scene)mountStage(false);
  window.scrollTo(0,0);
}
document.addEventListener('click',event=>{const a=event.target.closest('a');if(a&&a.origin===location.origin&&a.pathname.startsWith('/team')){event.preventDefault();history.pushState({},'',a.href);route();}});
addEventListener('popstate',route);
document.addEventListener('visibilitychange',()=>{if(document.hidden)stopDemo();});
function advanceReplay(last=false){
  if(!replay)return;
  const end=last?replay.events.length:Math.min(replay.events.length,replayIndex+1);
  for(;replayIndex<end;replayIndex++){
    const ev=replay.events[replayIndex];applyEvent(ev);
    text($('day-clock'),`历史 ${replay.trade_date} · ${ev.at.slice(11,19)}（该记录时刻）`);
    text($('evidence-source'),`${ev.provenance.source}\n记录 ${ev.provenance.id}\nSHA ${ev.provenance.sha256}\n观测 ${ev.at}`);
  }
  text($('replay-progress'),`${replayIndex} / ${replay.events.length} 条 · 末条不等于全天完整覆盖`);
  $('replay-next').disabled=replayIndex===replay.events.length;$('replay-last').disabled=$('replay-next').disabled;
}
$('replay-toggle').onclick=async()=>{
  if(replay)return;
  $('replay-toggle').disabled=true;
  try{
    const response=await fetch('/api/ui/replay?trade_date=2026-10-20');
    if(!response.ok)throw Error('FIXTURE_UNAVAILABLE');
    const data=await response.json();
    if(data.schema!=='team-ui-fixture/1'||data.kind!=='PARTIAL_FROZEN_EVIDENCE'||data.trade_date!=='2026-10-20')throw Error('FIXTURE_INVALID');
    stopDemo();sse?.close();sse=null;replay=data;replayIndex=0;
    $('replay-controls').hidden=false;$('demo-toggle').disabled=true;
    text($('connection'),'历史局部原件 · 非实时');text($('mode-notice'),'10-20 历史局部原件回放，不是当前运行状态。缺失数据仍为「—」。');
    text($('replay-coverage'),`${data.coverage.original_plans} 原计划；${data.coverage.team_sessions} Team时点；${data.coverage.condition_rows} 条件原件。缺口：${data.coverage.missing.join('；')}`);
    text($('replay-toggle'),'10-20 原件已载入');advanceReplay();
  }catch{ text($('mode-notice'),'本机冻结原件不可用或校验未通过，未接入任何生产数据。');$('replay-toggle').disabled=false; }
};
$('replay-next').onclick=()=>advanceReplay();$('replay-last').onclick=()=>advanceReplay(true);
// Explicit caller opt-in only; this local skeleton never opens a production stream.
function connectEventStream(path){
  const url=new URL(path,location.origin);if(url.origin!==location.origin||url.pathname!=='/api/ui/events')throw Error('UI_EVENT_STREAM_SCOPE_INVALID');
  sse?.close();sse=new EventSource(url);sse.onopen=()=>text($('connection'),'事件流已连接');
  sse.addEventListener('fixture-end',()=>{sse?.close();text($('connection'),'历史事件已读完 · 非实时');});
  sse.onmessage=message=>{try{applyEvent(JSON.parse(message.data));}catch{text($('connection'),'事件格式错误 · 保留最后已验证状态');}};
  sse.onerror=()=>text($('connection'),'事件流断开 · 正在重连');return ()=>{sse?.close();sse=null;};
}
window.liangjian=Object.freeze({applyEvent,setRole,bubble,stopDemo,connectEventStream});
render();
if(document.readyState==='complete')route();else addEventListener('load',route,{once:true});
