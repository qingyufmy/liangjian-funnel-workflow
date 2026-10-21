import {ROLES,STATES} from './roles.js';
export const EVENT_TYPES=Object.freeze(['clock','task','role','say','relay','focus','conds','account','plans','positions','risk','theme','alert','market']);
export function orderedPlans(rows){
  const priority={ACTIVE_TODAY:0,ACTIVE:0,PENDING_MORNING_REVIEW:1,PENDING:1,REJECTED:2,ABANDONED:2,EXPIRED:2};
  return rows.filter(row=>row.status!=='INVALIDATED').map((row,index)=>({row,index})).sort((a,b)=>(priority[a.row.status]??3)-(priority[b.row.status]??3)||a.index-b.index).map(item=>item.row);
}
export function addedPositions(previous,next){
  if(previous===null)return []; // Initial ledger synchronization is not a new fill.
  const ids=new Set(previous.map(r=>r.id??r.code));
  return next.filter(r=>!ids.has(r.id??r.code));
}
const ids=new Set(ROLES.map(r=>r.id));
export function emptyState(){return {day:null,hhmm:null,account:null,tasks:[],feed:[],focus:null,conds:null,relay:null,plans:null,positions:null,risk:null,theme:null,market:null,alerts:[],roles:Object.fromEntries(ROLES.map(r=>[r.id,{state:'idle',label:'缺少工作事件'}])),events:0};}
export function validateEvent(ev){
  if(!ev||typeof ev!=='object'||!EVENT_TYPES.includes(ev.type))throw Error('UI_EVENT_TYPE_INVALID');
  if(['role','say'].includes(ev.type)&&!ids.has(ev.id))throw Error('UI_ROLE_ID_INVALID');
  if(ev.type==='role'&&!STATES.includes(ev.state))throw Error('UI_ROLE_STATE_INVALID');
  if(ev.type==='say'&&typeof ev.text!=='string')throw Error('UI_SAY_TEXT_INVALID');
  if(ev.type==='relay'&&(!ids.has(ev.from)||!ids.has(ev.to)))throw Error('UI_RELAY_ID_INVALID');
  if(['plans','positions'].includes(ev.type)&&!Array.isArray(ev.rows))throw Error('UI_ROWS_INVALID');
  if(ev.type==='conds'&&(!Array.isArray(ev.met)||ev.met.some(v=>v!==null&&typeof v!=='boolean')))throw Error('UI_CONDITIONS_INVALID');
  if(ev.type==='account'&&['nav','cash','float','day_pct','week_pct'].some(k=>ev[k]!==undefined&&ev[k]!==null&&(!Number.isFinite(ev[k]))))throw Error('UI_ACCOUNT_VALUE_INVALID');
  if(ev.type==='clock'&&ev.hhmm&&!/^([01]\d|2[0-3]):[0-5]\d$/.test(ev.hhmm))throw Error('UI_CLOCK_INVALID');
  return ev;
}
export function reduceEvent(state,event){
  const ev=validateEvent(event),s={...state,events:state.events+1};
  switch(ev.type){
    case 'clock':s.day=ev.day??s.day;s.hhmm=ev.hhmm??s.hhmm;break;
    case 'role':s.roles={...s.roles,[ev.id]:{state:ev.state,label:ev.label??ev.state}};break;
    case 'task':s.tasks=[...s.tasks.filter(r=>r.id!==ev.id),{...ev}];break;
    case 'say':s.feed=[...s.feed,{...ev}].slice(-500);break;
    case 'alert':s.alerts=[...s.alerts,{...ev}].slice(-100);break;
    case 'plans':s.plans=ev.rows.map(r=>({...r}));break;
    case 'positions':s.positions=ev.rows.map(r=>({...r}));break;
    case 'relay':s.relay={...ev};s.roles={...s.roles,[ev.to]:{state:'work',label:'收到工作交接'}};break;
    case 'focus':case 'conds':case 'account':case 'risk':case 'theme':case 'market':s[ev.type]={...ev};break;
  }
  return s;
}
