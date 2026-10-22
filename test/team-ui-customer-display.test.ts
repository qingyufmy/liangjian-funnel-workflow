import {test,expect,vi} from 'vitest';
import {readFileSync} from 'node:fs';
// @ts-expect-error Framework-free display functions.
import {priceText,zoneText,riskUnitLabel,securityNames,securityText,annotateSecurities,dataStatusText,marketFacts,confirmationText,tradeRecordText,rrDisplay} from '../web/team/display.js';
// @ts-expect-error Framework-free chart geometry.
import {chartModel,openPlanDetail,conditionDisplayValue} from '../web/team/plan-detail.js';
// @ts-expect-error Framework-free state projection.
import {applyDashboard} from '../web/team/dashboard.js';
// @ts-expect-error Framework-free state reducer.
import {emptyState} from '../web/team/model.js';
// @ts-expect-error Framework-free metadata display.
import {orderedHealthEvents} from '../web/team/health.js';
test('security names join only explicit same identity rows, including orders fills and admissions',()=>{
 const state={plans:[{code:'002270.SZ',name:'华明装备'}],fills:[{symbol:'002270.SZ'}],orders:[],positions:[],admissions:[]};
 const before=JSON.stringify(state),names=securityNames(state);
 expect(securityText(state.fills[0],names)).toBe('华明装备 002270.SZ');
 expect(annotateSecurities('已读 paper:002270.SZ:fill',names)).toContain('002270.SZ（华明装备）');
 expect(annotateSecurities('002270 已触发',names)).toContain('华明装备');
 expect(securityText({code:'000001.SH'},names)).toBe('名称缺源 000001.SH');
 expect(JSON.stringify(state)).toBe(before);
});
test('name ambiguity never guesses exchange, text annotation avoids hashes and already paired name',()=>{
 const names=securityNames({plans:[{code:'000001.SH',name:'上证指数'},{code:'000001.SZ',name:'平安银行'}]});
 expect(annotateSecurities('000001 待核',names)).toBe('000001 待核');
 expect(annotateSecurities('平安银行 000001.SZ',names)).toBe('平安银行 000001.SZ');
 const digest='ab002270cd'.repeat(6);expect(annotateSecurities(digest,names)).toBe(digest);
 expect(securityNames({plans:[{code:'002270.SZ',name:'A'},{code:'002270.SZ',name:'B'}]}).size).toBe(0);
});
test('prices and zones use at most two display decimals without altering raw judgment values',()=>{
 const plan={zone:[12.3292,12.4525]},raw=JSON.stringify(plan);
 expect(zoneText(plan)).toBe('12.33–12.45');expect(priceText(18.725)).toBe('18.73');expect(priceText('18.725')).toBe('18.73');expect(priceText(null)).toBe('未提供');expect(priceText('0x20')).toBe('未提供');
 expect(zoneText({zone:'12.3292–12.4525'})).toBe('12.33–12.45');expect(zoneText({zone:[null,10]})).toContain('未提供');
 expect(JSON.stringify(plan)).toBe(raw);expect(riskUnitLabel('PROBE')).toBe('试探仓');
 expect(riskUnitLabel('STANDARD')).toBe('标准仓');expect(riskUnitLabel('NONE')).toBe('不适用');
});
test('missing research flags are not substituted by green live data; sourced statuses stay distinct',()=>{
 expect(dataStatusText({})).toBe('研究完整性标记缺源');
 expect(dataStatusText({data_status:{status:'AVAILABLE',label:'行情齐全'}})).not.toBe('行情齐全');
 expect(dataStatusText({data_status:{status:'PARTIAL',label:'缺历史5分钟线',source:{id:'actual',sha256:'a'.repeat(64)}}})).toBe('缺历史5分钟线');
 expect(dataStatusText({data_status:{status:'AVAILABLE',label:'等待形态条件',source:{id:'actual',sha256:'a'.repeat(64)}}})).toBe('等待形态条件');
 expect(dataStatusText({data_limits:{data_limited:false}})).not.toContain('完整');
});
test('market emotions need their own actual source; internal runtime health is not market mood',()=>{
 expect(marketFacts({health:{mode:'control',record_latch:false}}).stage).toBe('情绪数据缺源');
 expect(marketFacts({market:{emotion:{stage_label:'分化'}}}).stage).toBe('情绪数据缺源');
 expect(marketFacts({market:{emotion:{stage_label:'分化',observed_at:'2026-10-22T14:00:00+08:00',source:{id:'original',sha256:'a'.repeat(64)}}}}).stage).toBe('分化');
 const html=readFileSync('web/team/index.html','utf8');expect(html).toMatch(/id="internal-health" hidden inert aria-hidden="true"/);
 expect(readFileSync('web/team/studio.js','utf8')).not.toContain("['系统健康','health']");
});
test('confirmation count uses the supplied final sample time, never the best all-day count',()=>{
 const p={condition_as_of:'2026-10-22T06:57:00Z',conditions:[{status:'MET'},{status:'MET',source_valid:false},{status:'UNMET'}]};
 expect(confirmationText(p)).toBe('1/3 · 14:57');expect(confirmationText({...p,condition_as_of:null})).toContain('时刻缺源');
 const names=securityNames({dashboard:{security_names:{'002270.SZ':{name:'华明装备',source:{id:'old-plan-identity-only',sha256:'a'.repeat(64)}}}}});
 expect(securityText({symbol:'002270.SZ'},names)).toBe('华明装备 002270.SZ');
});
test('enlarged charts retain raw price geometry and gaps, with vector price and time axes',()=>{
 const input={code:'002270.SZ',name:'华明装备',stop:9.9234,zone_values:[10.2222,10.3456],bars:[{at:'2026-10-22T10:00:00+08:00',close:10.3123,closed:true},{at:'2026-10-22T10:10:00+08:00',close:10.3345,closed:true}]};
 const raw=JSON.stringify(input),model=chartModel(input);expect(model.coverage.gapped).toBe(true);expect(model.bars[0].close).toBe(10.3123);expect(JSON.stringify(input)).toBe(raw);
 const code=readFileSync('web/team/plan-detail.js','utf8');expect(code).toContain("viewBox:'0 0 380 180'");expect(code).toContain('model.coverage.first.slice');expect(code).toContain('model.min+(model.max-model.min)*f');
});
test('customer trading records use structured actual sources, not model-validation filler',()=>{
 const d={status:'AVAILABLE',sections:{},trade_records:[{role:'A4',at:'2026-10-22T14:55:00+08:00',text:'丝路视觉等待缩量条件',source:{id:'original',sha256:'a'.repeat(64)}},{text:'无源猜测'}]};
 expect(applyDashboard(emptyState(),d).feed).toHaveLength(1);
 expect(applyDashboard(emptyState(),d).feed[0].text).toBe('丝路视觉等待缩量条件');
 expect(applyDashboard(emptyState(),{status:'AVAILABLE',sections:{calls:{status:'AVAILABLE',rows:[{role:'A2',validated:true}]}}}).feed).toEqual([]);
 expect(tradeRecordText({kind:'PLAN_CONDITIONS',symbol:'300556.SZ',name:'丝路视觉',unmet_conditions:['TREND_PULLBACK_VOLUME_CONTRACTION']})).toBe('丝路视觉 300556.SZ：未满足：回调缩量');
 expect(tradeRecordText({kind:'PLAN_CONDITIONS',symbol:'600201.SH',name:'生物股份',unmet_conditions:['A_CONTRACTION','B_ATTEMPT_PREFIX'],unproven_conditions:['B_ATTEMPT_PREFIX']})).toBe('生物股份 600201.SH：未满足：回调缩量；缺证：开盘起的完整K线前缀');
 expect(tradeRecordText({kind:'PLAN_CONDITIONS',unmet_conditions:[],unproven_conditions:[]})).toBe('条件记录无缺项（不代表已触发信号）');
 expect(tradeRecordText({kind:'EXECUTION_FILL',symbol:'002270.SZ',name:'华明装备',qty:4160,side:'SELL',price:18.725})).toContain('华明装备 002270.SZ：卖出成交 4,160 股，价格 18.73');
 expect(tradeRecordText({kind:'EXECUTION_ADMISSION',symbol:'002270.SZ',name:'华明装备',shares:10500,status:'APPROVED',risk_unit:'NONE',reason_labels:['触及硬止损']})).toBe('华明装备 002270.SZ：执行准入批准 10,500 股 · 触及硬止损');
 const ledger=applyDashboard(emptyState(),{status:'AVAILABLE',sections:{},trade_records:[{role:'SYSTEM',text:'实际成交',source:{id:'fill',sha256:'a'.repeat(64)}}]});
 expect(ledger.feed[0]).toMatchObject({id:null,who:'系统账本'});
});
test('default full trading room and all three mobile plan rows are not collapsed or clipped',()=>{
 const html=readFileSync('web/team/index.html','utf8'),css=readFileSync('web/team/studio.css','utf8');
 expect(html).not.toContain('id="stage-toggle"');expect(html).toContain('id="scene-shell"><');
 expect(css).toContain('.studio-main{grid-template-columns:minmax(0,1fr)}');
 expect(css).toContain('.plans-panel .table-wrap{max-height:none}');
 expect(css).toContain('@media(max-width:1200px) and (min-width:761px)');expect(css).toContain('white-space:nowrap;overflow-wrap:normal;word-break:normal');
 const rows=[{at:null,id:1},{at:'2026-10-22T08:38:00+08:00',id:2},{at:'2026-10-22T08:35:00+08:00',id:3},{at:'bad',id:4}];
 expect(orderedHealthEvents(rows).map((x:{id:number})=>x.id)).toEqual([3,2,1,4]);expect(rows[0].id).toBe(1);
});
test('actual drawer render includes sourced RR and confirmation times, translated risk and named chart',()=>{
 const element=(tag:string):any=>({tag,children:[],dataset:{},attributes:{},textContent:'',open:false,
   append(...nodes:any[]){this.children.push(...nodes);},replaceChildren(...nodes:any[]){this.children=nodes;},
   insertBefore(node:any,before:any){const index=this.children.indexOf(before);if(index<0)throw Error('DOM_ANCHOR_MISSING');this.children.splice(index,0,node);},
   setAttribute(key:string,value:string){this.attributes[key]=value;},showModal(){this.open=true;},close(){this.open=false;},
   get firstChild(){return this.children[0];}});
 const body=element('body'),walk=(node:any):any[]=>[node,...node.children.flatMap(walk)];
 vi.stubGlobal('document',{body,createElement:element,createElementNS:(_ns:string,tag:string)=>element(tag),getElementById:(id:string)=>walk(body).find(n=>n.id===id)});
 try{
   openPlanDetail({code:'002354.SZ',name:'天娱数科',zone_values:[7.552,7.577142857],stop:7.3,target:8.27,rr:47.5,risk_unit:'PROBE',conditions:[],
     rr_basis:{label:'最新闭合观测',formula:'(目标−观测价)/(观测价−止损)',price:7.32,stop:7.3,target:8.27,observed_at:'2026-10-22T14:55:00+08:00',decision_at:'2026-10-22T14:57:00+08:00'},
     confirmation_basis:{closed_5m_end:'2026-10-22T14:55:00+08:00',decision_at:'2026-10-22T14:57:00+08:00',met_count:1,required_count:7},
     bars:[{at:'2026-10-22T14:55:00+08:00',close:7.32,closed:true}]});
   const nodes=walk(body),text=nodes.map(n=>n.textContent).join(' ');
   expect(text).toContain('天娱数科 002354.SZ');expect(text).toContain('7.55–7.58');expect(text).toContain('试探仓');
   expect(text).toContain('14:57:00');expect(text).toContain('实际满足 1/7');expect(text).not.toContain('7.577142857');
   expect(text).toContain('47.50（失真）');expect(text).toContain('按14:55价格计算');
   expect(nodes.find(n=>n.tag==='svg').attributes['aria-label']).toContain('天娱数科');
   expect(body.children[0].open).toBe(true);
 }finally{vi.unstubAllGlobals();}
});
test('RR distortion is a display flag from actual same-sample price/stop, never changes raw or thresholds',()=>{
 const p={rr:47.5,rr_basis:{price:7.32,stop:7.30,closed_bar_end:'2026-10-22T14:55:00+08:00'}};
 const raw=JSON.stringify(p),shown=rrDisplay(p);expect(shown.distorted).toBe(true);expect(shown.value).toContain('失真');expect(shown.clock).toBe('14:55');expect(JSON.stringify(p)).toBe(raw);
 expect(rrDisplay({rr:10,rr_basis:{price:100,stop:99.5}}).distorted).toBe(false);
 expect(rrDisplay({rr:10,rr_basis:{price:100,stop:100}}).distorted).toBe(false);
 expect(rrDisplay({rr:10}).distorted).toBe(false);expect(rrDisplay({rr:10}).explanation).toContain('缺源');
});
test('condition objects and arrays display at most two decimals without overwriting raw precision or inventing zero',()=>{
 const raw={high:7.577142857142857,low:'7.552',nested:[47.49999999999887,{volume:6340,missing:null}],available:false};
 const before=JSON.stringify(raw),shown=conditionDisplayValue(raw);
 expect(shown).toEqual({high:7.58,low:7.55,nested:[47.5,{volume:6340,missing:'未提供'}],available:false});
 expect(conditionDisplayValue(undefined)).toBe('未提供');expect(JSON.stringify(raw)).toBe(before);
 expect(readFileSync('web/team/plan-detail.js','utf8')).toContain('threshold:c.threshold,value:c.value');
});
