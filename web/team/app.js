import {ROLES,STATE_LABELS} from './roles.js';
import {emptyState,reduceEvent,addedPositions,orderedPlans} from './model.js';
import {createStage} from './stage.js';
import {conditionLabel,strategyLabel,planStatusLabel,clusterLabel} from './labels.js';
import {createPoller} from './poll.js';

const $=id=>document.getElementById(id);
const number=(v,digits=2)=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('zh-CN',{minimumFractionDigits:digits,maximumFractionDigits:digits}):'—';
const text=(node,value)=>{node.textContent=value==null?'—':String(value);};
const readonlyStatus=message=>{text($('connection'),message);const busy=message==='稍后刷新';$('refresh-status').hidden=!busy;text($('refresh-status'),busy?(state.events?'稍后刷新 · 保留上次数据':'稍后刷新'):'');};
const role=id=>ROLES.find(r=>r.id===id);
const taskStatus=value=>({VALIDATED:'校验通过',VALIDATION_FAILED:'校验未通过',ENGINE_TRACKING_NO_REVIEW_CANDIDATE:'引擎跟踪，没有需模型评审的候选'})[value]??'任务状态待说明';
const byText=(tag,value,className)=>{const el=document.createElement(tag);text(el,value);if(className)el.className=className;return el;};
let state=emptyState(),scene=null,reviewScene=null,demoTimer=null,sse=null,replay=null,replayIndex=0,expandedPlans=false,previewTimer=null,livePoller=null;
function stateIcon(value){
  const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 20 20');svg.setAttribute('aria-hidden','true');svg.classList.add('state-icon');
  const path=document.createElementNS(svg.namespaceURI,'path');
  path.setAttribute('d',value==='met'?'M3 10l4 4L17 4':value==='stop'?'M5 3h10l3 3v8l-3 3H5l-3-3V6z':value==='work'?'M5 3l12 7-12 7z':value==='unknown'?'M6 7a4 4 0 017-1c1 3-3 3-3 6m0 3v1':'M10 7a3 3 0 110 6 3 3 0 010-6');svg.append(path);return svg;
}
function fillRoles(root){
  root.replaceChildren(...ROLES.map(r=>{const item=document.createElement('article');item.dataset.role=r.id;const status=byText('span','','role-state'),action=byText('span','','role-action');status.hidden=true;action.hidden=true;status.append(byText('span','','role-dot'),byText('span','','role-state-text'));item.append(byText('strong',r.name),status,action);return item;}));
}
fillRoles($('role-fallback'));fillRoles($('review-fallback'));
const updateScrollHint=()=>{$('scene-shell').dataset.hasMore=String($('scene').scrollWidth-$('scene').clientWidth-$('scene').scrollLeft>2);};
$('scene').addEventListener('scroll',updateScrollHint,{passive:true});
new ResizeObserver(updateScrollHint).observe($('scene'));
new ResizeObserver(updateScrollHint).observe($('role-fallback'));
$('role-picks').replaceChildren(...ROLES.map(r=>{const b=byText('button',r.short);b.type='button';b.setAttribute('aria-label',`近看${r.name}`);b.onclick=()=>scene?.focus(r.id);return b;}));
$('overview').onclick=()=>scene?.focus('all');
for(const r of ROLES){const option=byText('option',r.name);option.value=r.id;$('preview-role').append(option);}
$('role-notes').replaceChildren(...ROLES.map(r=>{const article=document.createElement('article');article.append(byText('b',r.name),byText('p',r.cues[0]),byText('p',r.cues[1]),byText('p',`work：${r.work}`));return article;}));
function table(host,columns,rows,{compactEvidence=false}={}){
  if(!rows.length){host.replaceChildren(byText('p',host.id==='positions'?'还没有持仓':'今天暂无可执行计划','empty'));return;}
  const table=document.createElement('table'),head=document.createElement('thead'),tr=document.createElement('tr');
  for(const [,label]of columns)tr.append(byText('th',label));head.append(tr);table.append(head);
  const body=document.createElement('tbody');
  for(const row of rows){const line=document.createElement('tr');for(const [key]of columns){const cell=document.createElement('td');if(key==='security'){cell.append(byText('b',row.name??row.code),byText('small',row.code));}else if(key==='status'){const chip=byText('span',planStatusLabel(row.status),'status-chip');chip.dataset.status=row.status;cell.append(chip);}else cell.append(byText('span',key==='strat'?strategyLabel(row[key]):key==='cluster'?clusterLabel(row[key]):row[key]));line.append(cell);}const details=document.createElement('details');details.append(byText('summary','来源'),byText('p',`原计划：${row.source_id??'缺少记录ID'} · SHA ${row.source_sha256??'缺失'}`),byText('p',`原策略 ${row.strat??'未提供'}；原状态 ${row.status??'未提供'}；原分组 ${row.cluster??'未提供'}`));if(row.status_source_id)details.append(byText('p',`状态更新：${row.status_source_id} · ${row.status_source_at} · SHA ${row.status_source_sha256}`));if(compactEvidence)line.firstElementChild.append(details);else{const evidence=document.createElement('td');evidence.append(details);line.append(evidence);}body.append(line);}
  if(!compactEvidence)tr.append(byText('th','证据'));
  table.append(body);host.replaceChildren(table);
}
function renderPlans(){
  if(state.plans===null)return;
  const rows=orderedPlans(state.plans);text($('plan-count'),`${rows.length} 份 · 排除作废 ${state.plans.length-rows.length}`);
  table($('plans'),[['security','股票'],['strat','形态'],['zone','触发区'],['stop','止损'],['status','状态']],expandedPlans?rows:rows.slice(0,10),{compactEvidence:true});
  if(rows.length>10){const button=byText('button',expandedPlans?'收起至前 10 条':`展开全部 ${rows.length} 条`,'expand-plans');button.type='button';button.setAttribute('aria-expanded',String(expandedPlans));button.onclick=()=>{expandedPlans=!expandedPlans;renderPlans();};$('plans').append(button);}
}
function render(){
  text($('day-clock'),state.day?`交易日 ${state.day} · ${state.hhmm??'—'}`:'等待带来源的时钟事件');
  if(state.account){
    $('account-band').classList.remove('is-empty');$('account-empty').hidden=true;
    for(const id of ['account-total','account-metrics','account-provenance'])$(id).hidden=false;
    for(const [field,id]of[['nav','nav'],['cash','cash'],['float','float']])text($(`account-${id}`),number(state.account[field]));
    for(const field of['day','week']){const value=state.account[`${field}_pct`],el=$(`account-${field}`);text(el,value==null?'—':`${value>=0?'+':''}${number(value)}%`);el.className=field==='day'&&value!=null?(value>=0?'up':'down'):'';}
    $('account-float').className=state.account.float==null?'':state.account.float>=0?'up':'down';
    const p=state.account.provenance;text($('account-source'),p?.id&&p?.sha256?`观测 ${state.account.at??'时刻缺失'} · 来源 ${p.id} · SHA ${p.sha256}`:'已收到事件，来源记录未提供');
  }
  text($('account-counts'),`${state.plans===null?'—':state.plans.filter(r=>r.status!=='INVALIDATED').length} / — / —`);
  for(const r of ROLES){const current=state.roles[r.id],known=current.label!=='缺少工作事件';const article=$('role-fallback').querySelector(`[data-role="${r.id}"]`),status=article.querySelector('.role-state'),action=article.querySelector('.role-action');status.hidden=!known;action.hidden=!known;status.replaceChildren(byText('span','','role-dot'),byText('span',known?(STATE_LABELS[current.state]??'—'):'','role-state-text'));text(action,known?(current.label===current.state?STATE_LABELS[current.state]:current.label):'');}
  const working=ROLES.filter(r=>['work','talk'].includes(state.roles[r.id].state));
  text($('room-current'),working.length?working.map(r=>r.name).join('、'):state.events?'已读记录中未见正在工作状态':'当前暂无工作记录');
  const nextTask=state.tasks.find(t=>['queued','running','PENDING','RUNNING','now'].includes(t.status));
  text($('room-next'),nextTask?.label??'下一项工作将在调度后显示');
  const hasAttention=Boolean(state.risk?.latch_record||state.alerts.length);$('room-attention-column').hidden=!hasAttention;$('needs-attention').hidden=!state.alerts.length;
  text($('room-attention'),state.risk?.latch_record?'只记录保护锁已触发':state.alerts.length?'有异常记录待查看':'');
  scene?.setContext?.({market:typeof state.market?.stage_label==='string'?state.market.stage_label:undefined});
  text($('room-market'),typeof state.market?.stage_label==='string'?`市场 ${state.market.stage_label}`:'市场阶段 · 开盘后更新');
  text($('room-limits'),state.risk?.max_open!=null?`新开上限 ${state.risk.max_open} · 其他权限见原件`:'今日权限 · 晨会后更新');
  if(state.tasks.length){text($('task-count'),`${state.tasks.length} 条记录，列表内滚动查看全部`);$('tasks').replaceChildren(...state.tasks.slice().reverse().map(t=>{const li=document.createElement('li'),details=document.createElement('details');li.append(byText('time',t.at??t.time??'—'),byText('b',`${t.label==='Team A4'?'团队交易员':t.label??'任务'}：${taskStatus(t.status)}`),byText('small',t.note??'无附加说明'));details.append(byText('summary','原状态与来源'),byText('small',`${t.status}；${t.reason_codes?.join(', ')??''}；${t.provenance?.id??'记录未提供'}；SHA ${t.provenance?.sha256??'未提供'}`));li.append(details);return li;}));}
  if(state.focus){const f=state.focus;$('focus').replaceChildren(byText('h3',`${f.name??''} ${f.code??'标的未提供'}`),byText('p',`${strategyLabel(f.strat)}，现价 ${number(f.price)}。条件按本条原观测核对。`));text($('focus-clock'),f.at??'未提供观测时刻');}
  if(state.conds)$('conditions').replaceChildren(...state.conds.met.map((value,i)=>{const line=document.createElement('div');line.className='condition';line.append(stateIcon(value===null?'unknown':value?'met':'unmet'),byText('span',state.conds.labels?.[i]?conditionLabel(state.conds.labels[i]):`条件 ${i+1}（名称未提供）`,'condition-name'),byText('b',value===null?'证据不足':value?'满足':'未满足'));return line;}));
  text($('raw-contracts'),JSON.stringify({strategy:state.focus?.strat,condition_keys:state.conds?.labels,risk_reason:state.risk?.reason_code,risk_mode:state.risk?.latch_record?'record':undefined},null,2));
  if(state.risk){const r=state.risk;text($('risk'),`新开 ${r.open??'—'} / ${r.max_open??'—'}，风险 ${r.risk_pct??'—'}%，现金 ${r.cash_pct??'—'}%，同簇 ${r.cluster??'—'}。${r.latch_record?'只记录保护锁':r.blocked?'风控拦截':'未报告拦截，不代表执行链已验证'}`);}
  if(state.feed.length){text($('feed-count'),`${state.feed.length} 条`);$('feed').replaceChildren(...state.feed.slice(-25).reverse().map(f=>{const li=document.createElement('li');li.append(byText('b',role(f.id).name),byText('time',f.at??'未提供时刻'),byText('p',f.text));return li;}));}
  if(state.alerts.length){const title=byText('h2','需要你看');title.id='attention-title';$('needs-attention').replaceChildren(title,...state.alerts.slice(-5).map(a=>{const box=document.createElement('div'),details=document.createElement('details');box.append(byText('p',`${a.at??'时刻未提供'}，${a.who??'来源缺失'}：${a.text==='TEAM_MONITOR_DEGRADED'?'盘中资料异常，账户已切入只记录保护；原因为历史记录，不代表当前状态。':a.text??'说明缺失'}`));details.append(byText('summary','查看原件说明'),byText('p',a.text));box.append(details);return box;}));}
  renderPlans();
  if(state.positions!==null){text($('position-count'),`账本记录 ${state.positions.length}`);table($('positions'),[['code','代码'],['name','标的'],['qty','数量'],['entry','入场'],['price','现价'],['stop','止损'],['src','来源']],state.positions);}
  text($('event-status'),`已接收 ${state.events} 条事件 · 无写入操作`);
}
export function applyEvent(ev){
  const previousPositions=state.positions;
  state=reduceEvent(state,ev);
  if(ev.type==='role'&&!(ev.id==='tr'&&ev.state==='happy')){scene?.setRole(ev.id,ev.state);if(['work','talk'].includes(ev.state))scene?.setContext?.({currentRole:ev.id});}
  if(ev.type==='positions'){
    if(addedPositions(previousPositions,ev.rows).length)scene?.setRole('tr','happy');
  }
  if(ev.type==='say')bubble(ev.id,ev.text);
  if(ev.type==='relay'){
    const line=$('handoff-line');text(line,`${role(ev.from).name}把工作交给${role(ev.to).name}；${ev.at??'时刻未提供'}`);
    scene?.relay?.(ev.from,ev.to);
    scene?.setRole(ev.to,'work');
  }
  render();return state.events;
}
export function setRole(id,value,label){applyEvent({type:'role',id,state:value,label});}
export function bubble(id,message){
  const article=$('role-fallback').querySelector(`[data-role="${id}"]`);
  if(article)text(article.querySelector('.role-action'),message);
}
function stopDemo(){
  clearInterval(demoTimer);demoTimer=null;$('demo-toggle').setAttribute('aria-pressed','false');text($('demo-toggle'),'演示：关闭');
  text($('mode-notice'),replay?'10-20 历史局部原件回放，不是当前运行状态。缺失数据仍为「—」。':'本机样板，未连接生产；缺失数字保持未知。');
  for(const r of ROLES)scene?.setRole(r.id,state.roles[r.id].state);
}
$('demo-toggle').onclick=()=>{
  if(replay)return;
  if(demoTimer){stopDemo();return;}
  text($('mode-notice'),'角色动作演示 / 非生产：仅切换动作，不生成计划、信号、成交、发言或账户数字。');
  text($('demo-toggle'),'演示：角色动作');$('demo-toggle').setAttribute('aria-pressed','true');let index=0;
  demoTimer=setInterval(()=>{for(const r of ROLES)scene?.setRole(r.id,'idle');const from=ROLES[index%ROLES.length].id,to=ROLES[++index%ROLES.length].id;scene?.setRole(to,'work');scene?.relay?.(from,to);},1800);
};
$('theme-toggle').onclick=()=>{const dark=document.documentElement.dataset.theme!=='dark';document.documentElement.dataset.theme=dark?'dark':'light';text($('theme-toggle'),dark?'浅色':'深色');};
let reviewPaused=true;
function updatePreview(){
  clearTimeout(previewTimer);
  reviewScene?.setView($('preview-mode').value);$('review-scene').parentElement.dataset.view=$('preview-mode').value;
  reviewScene?.focus($('preview-role').value);reviewScene?.setAll($('preview-state').value);
  for(const item of $('review-fallback').querySelectorAll('.role-state')){item.hidden=false;item.replaceChildren(byText('span','','role-dot'),byText('span',STATE_LABELS[$('preview-state').value]??'—'));}
}
for(const id of['preview-mode','preview-state','preview-role'])$(id).onchange=updatePreview;
$('preview-pause').onclick=()=>{reviewPaused=!reviewPaused;reviewScene?.pause(reviewPaused);$('preview-pause').setAttribute('aria-pressed',String(reviewPaused));text($('preview-pause'),reviewPaused?'动作已暂停':'暂停动作');};
$('preview-relay').onclick=()=>{clearTimeout(previewTimer);reviewScene?.setAll('idle');for(const item of $('review-fallback').querySelectorAll('.role-state'))text(item,'动作预览：待命');const card=$('review-fallback').querySelector('[data-role="rk"] .role-state');text(card,'动作预览：工作中');reviewScene?.relay?.('pm','rk');reviewScene?.setRole('rk','work');previewTimer=setTimeout(()=>{reviewScene?.setRole('rk','stop');text(card,'动作预览：拦截中');},900);};
function mountStage(review=false){
  const host=$(review?'review-scene':'scene'),fallback=$(review?'review-fallback':'role-fallback');
  const value=createStage(host,{preview:review,onUnavailable:()=>{host.classList.add('stage-unavailable');host.setAttribute('role','group');host.querySelector('canvas')?.setAttribute('hidden','');fallback.classList.add('is-primary');}});
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
    stopDemo();livePoller?.stop();sse?.close();sse=null;replay=data;replayIndex=0;
    $('replay-controls').hidden=false;$('demo-toggle').disabled=true;
    text($('connection'),'历史局部原件 · 非实时');text($('mode-notice'),'10-20 历史局部原件回放，不是当前运行状态。缺失数据仍为「—」。');
    text($('replay-coverage'),`${data.coverage.original_plans} 原计划；${data.coverage.team_sessions} Team时点；${data.coverage.condition_rows} 条件原件。缺口：${data.coverage.missing.join('；')}`);
    text($('replay-toggle'),'10-20 原件已载入');advanceReplay();
  }catch{ text($('mode-notice'),'本机冻结原件不可用或校验未通过，没有读取生产数据。');$('replay-toggle').disabled=false; }
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
async function startReadonlyPoll(){
  try{
    const response=await fetch('/api/ui/session',{credentials:'same-origin',signal:AbortSignal.timeout(2000)});
    if(response.status===401){location.assign('/team/login');return;}
    if(!response.ok)return; // Local-only preview has no authenticated session API.
    const session=await response.json();if(session.schema!=='team-ui-session/1'||!session.poll)return;
    stopDemo();$('demo-toggle').hidden=true;$('replay-toggle').hidden=true;
    document.querySelector('.sample-badge').textContent='只读';
    text($('mode-notice'),'内部模拟盘只读页面；未知数值不作推算。事件来源覆盖见任务证据。');
    const day=new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());
    let plansLoaded=false;
    $('plans').replaceChildren(byText('p','正在读取计划，繁忙时稍后刷新','empty'));
    const loadPlans=async()=>{
      const response=await fetch(`/api/ui/day/${day}/plans?limit=500`,{credentials:'same-origin',signal:AbortSignal.timeout(2000)});
      if(!response.ok)return response;
      const plans=await response.json();
      if(plans.status!=='AVAILABLE'||!Array.isArray(plans.rows))throw Error('PLAN_SOURCE_UNAVAILABLE');
      applyEvent({type:'plans',rows:plans.rows.map(row=>({...row,zone:Array.isArray(row.zone)?row.zone.join('–'):row.zone,source_id:row.source?.id,source_sha256:row.source?.sha256}))});plansLoaded=true;return response;
    };
    livePoller=createPoller({
      fetchPage:async since=>{if(!plansLoaded){const response=await loadPlans();if(!response.ok)return response;}return fetch(`/api/ui/events?trade_date=${day}&since_id=${since}&limit=100`,{credentials:'same-origin',signal:AbortSignal.timeout(2000)});},
      onRows:rows=>{for(const row of rows)applyEvent(row);},
      onStatus:readonlyStatus,onUnauthorized:()=>location.assign('/team/login')
    });
    try{const initial=await loadPlans();if(initial.status===401){location.assign('/team/login');return;}if(!initial.ok)readonlyStatus('稍后刷新');}catch{readonlyStatus('稍后刷新');}
    livePoller.start();
    document.addEventListener('visibilitychange',()=>document.hidden?livePoller.stop():livePoller.start());
    addEventListener('pagehide',()=>livePoller.stop(),{once:true});
  }catch{readonlyStatus('稍后刷新');}
}
render();
void startReadonlyPoll();
if(document.readyState==='complete')route();else addEventListener('load',route,{once:true});
