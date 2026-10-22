/** Allowlisted API → page state. Never fills missing metrics with zero or mock. */
import {tradeRecordText} from './display.js';
export function applyDashboard(state,data){
  if(data?.status!=='AVAILABLE'||!data.sections)throw Error('DASHBOARD_SOURCE_INVALID');
  const s={...state,dashboard:data,day:data.trade_date,hhmm:data.observed_at?.slice(11,16),account:data.account??null,market:data.market??null};
  if(data.risk)s.risk={...data.risk,latch_record:data.health?.record_latch===true};
  const plans=data.sections.plans;
  if(plans?.status==='AVAILABLE'&&Array.isArray(plans.rows))s.plans=plans.rows.map(row=>planAt(row,data.observed_at));
  const positions=data.sections.positions;
  if(positions?.status==='AVAILABLE'&&Array.isArray(positions.rows))s.positions=positions.rows.map(row=>({...row,entry:row.avg_cost,src:row.source?.id}));
  s.orders=data.sections.orders?.status==='AVAILABLE'?data.sections.orders.rows:state.orders??null;
  s.fills=data.sections.fills?.status==='AVAILABLE'?data.sections.fills.rows:state.fills??null;
  s.admissions=data.admissions??null;
  s.alerts=(data.alerts??[]).map(x=>({...x,who:'只读数据源'}));
  // Customer activity is only structured trading facts, not generic model
  // validation chatter. An absent source stays an empty record feed.
  s.feed=(data.trade_records??[]).filter(x=>x.source?.id&&x.source?.sha256&&typeof x.text==='string').slice(-100).map(x=>({id:({A1:'a1',A2:'a2',A3:'pm',PM:'pm',A4:'tr',A6:'rk',A5:'rv'})[x.role]??null,who:({A1:'基本面研究',A2:'题材研究',A3:'计划评审',PM:'组合经理',A4:'交易员',A6:'风控官',A5:'复盘官',SYSTEM:'系统账本'})[x.role]??'原件记录',at:x.at,text:tradeRecordText(x),provenance:x.source}));
  const roles={A1:'a1',A2:'a2',PM:'pm',A4:'tr',A6:'rk',A5:'rv'};
  s.roles={...state.roles};
  for(const agent of data.agents??[]){const id=roles[agent.agent];if(id&&agent.last)s.roles[id]={state:agent.last.validated?'rest':'stop',status_label:agent.state,label:`${agent.state} · ${agent.last.captured_at?.slice(11,19)??'时间缺失'}`};}
  return s;
}
export function planAt(row,observedAt){
  return {...row,stored_status:row.status,status:row.expires_at&&Date.parse(row.expires_at)<=Date.parse(observedAt)&&['ACTIVE','ACTIVE_TODAY','PENDING','PENDING_MORNING_REVIEW'].includes(row.status)?'EXPIRED':row.status,zone_values:row.zone,zone:Array.isArray(row.zone)?row.zone.join('–'):row.zone,source_id:row.source?.id,source_sha256:row.source?.sha256};
}
export function panelClock(section){
  return `数据时间 ${chinaClock(section?.observed_at)}${section?.status==='MISSING_SOURCE'?' · 缺数据':''}`;
}
export function chinaClock(value){
  if(typeof value!=='string'||!/(?:Z|[+-]\d{2}:\d{2})$/.test(value)||!Number.isFinite(Date.parse(value)))return '未知';
  return new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).format(new Date(value));
}
