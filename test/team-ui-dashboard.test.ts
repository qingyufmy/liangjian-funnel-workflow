import {execFileSync} from 'node:child_process';
import {mkdtempSync,mkdirSync,readFileSync,readdirSync,rmSync,cpSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname,basename} from 'node:path';
import {createHash} from 'node:crypto';
import {afterAll,beforeAll,expect,test,vi,afterEach} from 'vitest';
// @ts-expect-error Static browser JS.
import {applyDashboard,chinaClock,panelClock,planAt} from '../web/team/dashboard.js';
// @ts-expect-error Static browser JS.
import {emptyState} from '../web/team/model.js';
// @ts-expect-error Static browser JS.
import {createPoller,freshness} from '../web/team/poll.js';
const python=process.platform==='win32'?'python':'python3';let root:string;
const hash=()=>createHash('sha256').update(readFileSync(join(root,'state/team_v2/paper.db'))).digest('hex');let originalHash:string;
beforeAll(()=>{
 root=mkdtempSync(join(tmpdir(),'team-ui-dashboard-'));mkdirSync(join(root,'state/team_v2'),{recursive:true});
 execFileSync(python,['-B','-c',`import sqlite3,json,sys
from pathlib import Path
p=Path(sys.argv[1]);w=sqlite3.connect(p/'state/workflow.sqlite3')
w.execute('CREATE TABLE execution_plans(plan_id,symbol,status,valid_from,expires_at,updated_at,payload_json)')
w.execute('INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?)',('plan1','002354.SZ','EXPIRED','2026-10-22T09:26:00+08:00','2026-10-22T15:00:00+08:00','2026-10-22T15:00:00+08:00',json.dumps({'company_name':'天娱数科','trigger_low':'5.1','trigger_high':'5.2','stop_level':'5','risk_unit':'PROBE'})))
w.commit();w.close()
c=sqlite3.connect(p/'state/team_v2/paper.db')
c.executescript('CREATE TABLE paper_meta(key,value); CREATE TABLE paper_lots(lot_id,symbol,plan_id,open_date,qty,cost); CREATE TABLE paper_marks(symbol,price,trade_date,minute_end); CREATE TABLE paper_orders(order_id,symbol,plan_id,trade_date,status,body_json); CREATE TABLE paper_fills(fill_id,order_id,symbol,plan_id,side,qty,price,fee,minute_end);')
c.executemany('INSERT INTO paper_meta VALUES(?,?)',[('cash','999243.25'),('realized','-756.75')])
c.execute('INSERT INTO paper_lots VALUES(?,?,?,?,?,?)',('closed','002270.SZ','old-plan','2026-10-21',0,'0'))
for i,q,price in [(1,6340,'18.74'),(2,4160,'18.725')]:c.execute('INSERT INTO paper_fills VALUES(?,?,?,?,?,?,?,?,?)',('fill'+str(i),'order1','002270.SZ','old-plan','SELL',q,price,'1','2026-10-22T09:32:00+08:00'))
c.commit();c.close()
m=sqlite3.connect(p/'state/team_v2/model.db');m.execute('CREATE TABLE model_calls(call_id,role,session,captured_at,latency_ms,provider_attempts,validated,output_hash)')
for i,ok in enumerate([1,0]):m.execute('INSERT INTO model_calls VALUES(?,?,?,?,?,?,?,?)',('call'+str(i),'A4','INTRADAY','2026-10-22T09:35:00+08:00',100+i*100,1,ok,'a'*64))
m.commit();m.close()`,root]);originalHash=hash();
});
afterAll(()=>{if(dirname(resolve(root))!==resolve(tmpdir())||!basename(root).startsWith('team-ui-dashboard-'))throw Error('UNSAFE_TEST_CLEANUP');rmSync(root,{recursive:true,force:true,maxRetries:10,retryDelay:50});});
afterEach(()=>vi.useRealTimers());
function read(){return JSON.parse(execFileSync(python,['-B',resolve('scripts/team_ui_readonly.py'),root,JSON.stringify({kind:'dashboard',trade_date:'2026-10-22',limit:50,offset:0,days:20})],{encoding:'utf8'}));}
test('closed position NAV is current cash, preserves both partial fills and source SHA, never source prose',()=>{
 const d=read();expect(d.account).toMatchObject({cash:999243.25,nav:999243.25,realized:-756.75,position_pct:0});expect(d.sections.positions.rows).toEqual([]);
 expect(d.sections.fills.rows.map((f:{qty:number,price:string})=>[f.qty,f.price])).toEqual([[6340,'18.74'],[4160,'18.725']]);expect(d.metrics).toMatchObject({model_calls:2,model_latency_p50_ms:100,model_latency_max_ms:200,signals:null});
 expect(d.account.provenance.sha256).toMatch(/^[a-f0-9]{64}$/);expect(d.health.mode).toBeNull();expect(hash()).toBe(originalHash);expect(readdirSync(join(root,'state/team_v2')).sort()).toEqual(['model.db','paper.db']);
});
test('dashboard projection keeps real zero different from missing; T1 and fields map without mutation',()=>{
 const d=read(),before=JSON.stringify(d),state=applyDashboard(emptyState(),d);expect(state.account.nav).toBe(999243.25);expect(state.plans[0]).toMatchObject({code:'002354.SZ',name:'天娱数科',risk_unit:'PROBE',status:'EXPIRED'});expect(state.positions).toEqual([]);expect(state.fills).toHaveLength(2);expect(state.feed).toHaveLength(2);expect(state.roles.tr.state).toBe('stop');expect(JSON.stringify(d)).toBe(before);
 expect(()=>applyDashboard(emptyState(),{status:'MISSING_SOURCE'})).toThrow();
});
test('nonempty mixed-day positions never close the shared paper read transaction before NAV/orders/fills',()=>{
 const copy=mkdtempSync(join(tmpdir(),'team-ui-dashboard-held-'));cpSync(root,copy,{recursive:true});
 try{
  execFileSync(python,['-B','-c',`import sqlite3,sys
from pathlib import Path
c=sqlite3.connect(Path(sys.argv[1])/'state/team_v2/paper.db')
c.executemany('INSERT INTO paper_lots VALUES(?,?,?,?,?,?)',[('old','002354.SZ','plan1','2026-10-21',100,'1100'),('new','002354.SZ','plan1','2026-10-22',50,'550')])
c.execute('INSERT INTO paper_marks VALUES(?,?,?,?)',('002354.SZ','12.1','2026-10-22','2026-10-22T14:59:00+08:00'))
c.commit();c.close()`,copy]);
  const before=createHash('sha256').update(readFileSync(join(copy,'state/team_v2/paper.db'))).digest('hex');
  const d=JSON.parse(execFileSync(python,['-B',resolve('scripts/team_ui_readonly.py'),copy,JSON.stringify({kind:'dashboard',trade_date:'2026-10-22',limit:50,offset:0,days:20})],{encoding:'utf8'}));
  expect(d.account).toMatchObject({cash:999243.25,nav:1001058.25,float:165});expect(d.sections.positions.rows[0]).toMatchObject({qty:150,sellable_qty:100,t1_locked_qty:50});expect(d.sections.orders.status).toBe('AVAILABLE');expect(d.sections.fills.rows).toHaveLength(2);expect(d.account_consistency).toMatch(/ONE_PAPER_DATABASE/);expect(createHash('sha256').update(readFileSync(join(copy,'state/team_v2/paper.db'))).digest('hex')).toBe(before);
 }finally{if(dirname(resolve(copy))!==resolve(tmpdir())||!basename(copy).startsWith('team-ui-dashboard-held-'))throw Error('UNSAFE_TEST_CLEANUP');rmSync(copy,{recursive:true,force:true,maxRetries:10,retryDelay:50});}
});
test('30s freshness is transport success based, including empty increments, not event time',async()=>{
 vi.useFakeTimers();const statuses:unknown[]=[];const page=vi.fn();const poll=createPoller({fetchPage:async()=>({ok:true,status:200,json:async()=>({rows:[],next_since_id:0,dashboard:{}})}),onRows:vi.fn(),onPage:page,onStatus:(_s:string,m:unknown)=>statuses.push(m),onUnauthorized:vi.fn()});poll.start();await vi.advanceTimersByTimeAsync(10000);expect(page).toHaveBeenCalledTimes(1);const last=(statuses.at(-1)as {received_at:number}).received_at;expect(freshness(last,last+29999)).toBe('CURRENT');expect(freshness(last,last+30000)).toBe('STALE');poll.stop();
});
test('all panel clocks normalize UTC to Shanghai and missing timezone remains unknown',()=>{
 expect(chinaClock('2026-10-22T09:05:03.000Z')).toBe('17:05:03');expect(chinaClock('2026-10-22T17:05:03+08:00')).toBe('17:05:03');expect(chinaClock('2026-10-22T17:05:03')).toBe('未知');expect(panelClock({observed_at:'2026-10-22T09:05:03Z',status:'MISSING_SOURCE'})).toBe('数据时间 17:05:03 · 缺数据');
});
test('plan detail refresh uses the same expiry projection as the table without mutating stored ACTIVE',()=>{
 const row={status:'ACTIVE_TODAY',expires_at:'2026-10-22T15:00:00+08:00',zone:[10,11]};const projected=planAt(row,'2026-10-22T16:35:00+08:00');expect(projected.status).toBe('EXPIRED');expect(projected.stored_status).toBe('ACTIVE_TODAY');expect(row.status).toBe('ACTIVE_TODAY');expect(planAt(row,'2026-10-22T14:59:00+08:00').status).toBe('ACTIVE_TODAY');
});
test('per-order quantity_audit is allowlisted original arithmetic, never filled from current configuration',()=>{
 const code=String.raw`import sys,json
sys.path.insert(0,'scripts')
from team_ui_dashboard import quantity_audit
from team_ui_readonly import number,scalar
source={'raw_risk_qty':'107142.8571428571428571428571','cap20_qty':10600,'probe10_qty':5300,'final_qty':5300,'binding_limits':['PROBE_NOTIONAL_CAP'],'secret':'DO_NOT_EXPORT','nav':'1000000'}
before=json.dumps(source,sort_keys=True)
projected=quantity_audit(source,number,scalar)
print(json.dumps({'projection':projected,'missing':quantity_audit(None,number,scalar),'unchanged':before==json.dumps(source,sort_keys=True)}))`;
 const d=JSON.parse(execFileSync(python,['-B','-c',code],{encoding:'utf8'}));expect(d.projection).toMatchObject({cap20_qty:10600,probe10_qty:5300,final_qty:5300});expect(d.projection.raw_decimal_strings.raw_risk_qty).toBe('107142.8571428571428571428571');expect(d.missing).toBeNull();expect(d.unchanged).toBe(true);expect(JSON.stringify(d)).not.toContain('DO_NOT_EXPORT');
});
test('busy timeout and offline have different states, preserve cursor and reset after recovery',async()=>{
 vi.useFakeTimers();const states:string[]=[];let n=0;const poll=createPoller({fetchPage:async()=>{n++;if(n===1)return {status:503,ok:false};if(n===2)throw new TypeError('network down');return {status:200,ok:true,json:async()=>({rows:[],next_since_id:0})};},onRows:vi.fn(),onStatus:(_s:string,m:{state:string})=>states.push(m.state),onUnauthorized:vi.fn()});poll.start();await vi.advanceTimersByTimeAsync(10000);await vi.advanceTimersByTimeAsync(20000);await vi.advanceTimersByTimeAsync(9999);expect(states).toEqual(['BUSY','OFFLINE']);await vi.advanceTimersByTimeAsync(1);expect(states).toEqual(['BUSY','OFFLINE','CURRENT']);expect(poll.cursor()).toBe(0);poll.stop();
});
