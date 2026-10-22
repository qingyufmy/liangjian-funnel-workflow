/** §4 GET signatures for local evidence only. No SQL, model, notification or business imports. */
const roles=new Set(['a1','a2','pm','tr','rk','rv']);
const prefix='/api/ui/';
export function parseMockRoute(path,query){
  const day=/^\/api\/ui\/day\/(\d{4}-\d{2}-\d{2})\/(tasks|feed|plans|fills)$/.exec(path);
  const person=/^\/api\/ui\/role\/([a-z0-9]+)\/(today|stats)$/.exec(path);
  let route;
  if(day)route={kind:day[2],trade_date:day[1]};
  else if(person){if(!roles.has(person[1]))return {error:404};route={kind:'role-'+person[2],role:person[1],trade_date:'2026-10-20'};}
  else if(['review/scoreboard','review/lessons','settings'].includes(path.slice(prefix.length)))route={kind:path.slice(prefix.length),trade_date:'2026-10-20'};
  else if(['replay','snapshot','events'].includes(path.slice(prefix.length)))route={kind:path.slice(prefix.length),trade_date:query.get('trade_date')};
  else return {error:404};
  if(route.trade_date!=='2026-10-20')return {error:404};
  const integer=(key,fallback,min,max)=>{const raw=query.get(key);if(raw===null)return fallback;if(!/^\d+$/.test(raw))throw Error('QUERY_INVALID');const value=Number(raw);if(!Number.isSafeInteger(value)||value<min||value>max)throw Error('QUERY_INVALID');return value;};
  try{route.limit=integer('limit',50,1,500);route.offset=integer('offset',0,0,1_000_000);route.days=integer('days',20,1,20);}catch{return {error:400};}
  if(query.has('role')&&!roles.has(query.get('role')))return {error:400};
  return route;
}
export function projectMockResponse(route,fixture){
  const base={schema:'team-ui-readonly/1',data_mode:'LOCAL_HISTORICAL_FIXTURE',trade_date:fixture.trade_date,coverage_kind:fixture.kind,sources:fixture.sources};
  const list=(rows,status='AVAILABLE')=>({...base,status,rows:rows===null?null:rows.slice(route.offset,route.offset+route.limit),total:rows===null?null:rows.length,next_offset:rows&&route.offset+route.limit<rows.length?route.offset+route.limit:null});
  if(['replay','snapshot','events'].includes(route.kind))return fixture;
  if(route.kind==='tasks')return list(fixture.events.filter(e=>e.type==='task'));
  if(route.kind==='plans')return list(fixture.events.filter(e=>e.type==='plans').at(-1).rows);
  if(route.kind==='fills')return fixture.coverage.paper_counts.paper_fills===0?list([]):list(null,'MISSING_SOURCE');
  if(route.kind==='role-today'){
    const rows=fixture.events.filter(e=>e.type==='role'&&e.id===route.role);
    return list(rows.length?rows:null,rows.length?'AVAILABLE':'MISSING_SOURCE');
  }
  // Missing conversations, multi-day stats and config are not a zero-valued dataset.
  return {...list(null,'MISSING_SOURCE'),reason:'当前局部原件没有此项来源，未调用任何补采、研究或写入入口'};
}
