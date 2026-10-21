/** Whitelist projection of existing local evidence. No business imports or I/O. */
const numeric=value=>value===null||value===undefined?null:Number.isFinite(Number(value))?Number(value):null;
export function projectFrozenEvidence({morning,models,latest},sources){
  const day='2026-10-20',events=[];
  if(morning.trade_date!==day||!latest.at.startsWith(day))throw Error('FIXTURE_TRADE_DATE_MISMATCH');
  const source=(key,id,rowHash)=>({source:key,id,sha256:rowHash??sources[key].sha256});
  const emit=(at,event,provenance)=>{if(!at?.startsWith(day))throw Error('FIXTURE_EVENT_TIME_MISSING');events.push({...event,at,provenance});};
  const snapshot=morning.snapshot,at=morning.observed_at;
  emit(at,{type:'account',nav:numeric(snapshot.account.nav),cash:numeric(snapshot.account.cash),float:numeric(snapshot.account.pnl_unrealized)},source('morning','morning:account'));
  if(snapshot.positions.length===0)emit(at,{type:'positions',rows:[]},source('morning','morning:positions'));
  const catalog=JSON.parse(models.model_call_evidence[0].catalog_json);
  const original=catalog.items.filter(r=>r.category==='PLAN');
  if(original.length!==33)throw Error('FIXTURE_ORIGINAL_PLAN_COUNT_MISMATCH');
  const plans=original.map(({id,value:v})=>{const p=v.publication_facts;return {id:v.plan_id,code:v.symbol,name:p.company_name??p.name??null,strat:p.strategy_profile,cluster:v.cluster,zone:`${p.trigger_low}–${p.trigger_high}`,stop:p.stop_level,status:v.status,source_id:id,source_sha256:v.source_row_sha256};});
  emit(at,{type:'plans',rows:plans},source('models','catalog:PLAN',models.model_call_evidence[0].catalog_sha256));
  for(const call of models.model_calls){
    const issues=JSON.parse(call.issues_json??'[]');
    emit(call.captured_at,{type:'task',id:call.call_id,label:call.role==='A6'?'风险晨会':call.role,status:call.validated?'VALIDATED':'VALIDATION_FAILED',note:`耗时 ${(call.latency_ms/1000).toFixed(3)} 秒，${call.provider_attempts??'未知'} 次请求`,reason_codes:issues},source('models',call.call_id));
    const id=({A6:'rk',PM:'pm',A4:'tr',A5:'rv',A2:'a2',A1:'a1'})[call.role];
    // Model/schema failure is not an execution risk stop. Keep the failure in task text.
    if(id)emit(call.captured_at,{type:'role',id,state:'idle',label:call.validated?'校验通过':'校验未通过，非账户风控拦截'},source('models',call.call_id));
  }
  const planState=new Map(plans.map(p=>[p.id,p]));
  const groups=new Map();for(const row of latest.plans.metadata){if(!groups.has(row.updated_at))groups.set(row.updated_at,[]);groups.get(row.updated_at).push(row);}
  for(const [time,rows]of [...groups].sort(([a],[b])=>a.localeCompare(b))){
    for(const row of rows){const p=planState.get(row.plan_id);if(!p)throw Error('FIXTURE_UNKNOWN_PLAN');planState.set(row.plan_id,{...p,status:row.status,status_source_id:`execution_plans:${row.plan_id}`,status_source_at:row.updated_at,status_source_sha256:sources.latest.sha256});}
    emit(time,{type:'plans',rows:[...planState.values()].map(p=>({...p}))},source('latest',`execution_plans:${time}`));
  }
  for(const session of latest.team_sessions){
    const match=/^team:(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})$/.exec(session.decision_id);
    if(!match)continue;
    const time=`${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:00+08:00`;
    emit(time,{type:'task',id:session.decision_id,label:'Team A4',status:session.result.status,note:`候选 ${session.candidate_review.candidate_count}，模型对象 ${session.candidate_review.actual_model_objects}。未调用模型不代表模型主动观望。`},source('latest',session.decision_id,session.input_sha256));
  }
  for(const latch of latest.record_latch){
    emit(latch.body.observed_at,{type:'risk',latch_record:latch.body.mode==='record',reason_code:latch.body.reason_code},source('latest',`record-latch:${day}`,latch.body_sha256));
    emit(latch.body.observed_at,{type:'alert',who:'账户保护锁',text:latch.body.reason_code},source('latest',`record-latch:${day}`,latch.body_sha256));
    emit(latch.body.observed_at,{type:'role',id:'rk',state:'stop',label:'只记录保护锁'},source('latest',`record-latch:${day}`,latch.body_sha256));
  }
  for(const row of latest.condition_details){
    const proof=source('latest',`monitor_events:${row.event_id}`,row.payload_sha256);
    const conditions=Object.entries(row.confirmation_results);
    emit(row.as_of,{type:'focus',code:row.symbol,strat:row.strategy_profile,price:row.reference_price,reason_code:row.reason_code,closed_5m_end:row.closed_5m_end},proof);
    emit(row.as_of,{type:'conds',labels:conditions.map(([key])=>key),met:conditions.map(([,v])=>v.available?v.met:null)},proof);
  }
  if(latest.paper_counts.paper_lots===0)emit(latest.at,{type:'positions',rows:[]},source('latest','paper_lots:count'));
  events.sort((a,b)=>Date.parse(a.at)-Date.parse(b.at));
  return {schema:'team-ui-fixture/1',trade_date:day,kind:'PARTIAL_FROZEN_EVIDENCE',sources,coverage:{original_plans:33,plan_states:latest.plans.states,team_sessions:latest.team_sessions.length,condition_rows:latest.condition_details.length,notifications:latest.team_notification_counts.map(({kind,status,result_code,n})=>({kind,status,result_code,n})),paper_counts:{...latest.paper_counts},missing:['完整逐分钟旧客户A4流水','早晨PM模型原件（此包仅含A6原调用）','13类文字与14枚举待统一','theme/market/say/relay无对应来源不生成','非完整全天事件流，不推定日收益或执行链畅通']},events};
}
