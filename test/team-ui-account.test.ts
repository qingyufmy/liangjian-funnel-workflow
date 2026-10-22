import {execFileSync} from 'node:child_process';
import {mkdtempSync,mkdirSync,readFileSync,readdirSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname,basename} from 'node:path';
import {createHash} from 'node:crypto';
import {beforeAll,afterAll,expect,test} from 'vitest';
import {parseTeamUiQuery} from '../server/team-ui-readonly.js';
const python=process.platform==='win32'?'python':'python3';let root:string,before:string;
const digest=()=>createHash('sha256').update(readFileSync(join(root,'state/team_v2/paper.db'))).digest('hex');
beforeAll(()=>{
 root=mkdtempSync(join(tmpdir(),'team-ui-account-'));mkdirSync(join(root,'state/team_v2'),{recursive:true});
 execFileSync(python,['-B','-c',`import sqlite3,json,sys
from pathlib import Path
c=sqlite3.connect(Path(sys.argv[1])/'state/team_v2/paper.db')
c.execute('CREATE TABLE paper_lots(lot_id,symbol,plan_id,open_date,qty,cost)')
for row in [('old','002270.SZ','owner:1','2026-10-21',10500,'197316.82'),('new','002270.SZ','owner:1','2026-10-22',100,'1800'),('other-owner','002270.SZ','owner:2','2026-10-21',300,'5400')]:c.execute('INSERT INTO paper_lots VALUES(?,?,?,?,?,?)',row)
c.execute('CREATE TABLE paper_orders(order_id,symbol,plan_id,trade_date,status,body_json)')
for id,owner,status,side,qty in [('reserve','owner:1','OPEN','SELL',200),('done','owner:1','FILLED','SELL',100),('buy','owner:2','PARTIAL','BUY',100)]:
 c.execute('INSERT INTO paper_orders VALUES(?,?,?,?,?,?)',(id,'002270.SZ',owner,'2026-10-22',status,json.dumps({'side':side,'qty':qty,'remaining_qty':qty,'limit_price':'18.2','submitted_at':'2026-10-22T09:31:00+08:00','webhook':'SECRET','raw_response':'SECRET'})))
c.commit();c.close()`,root]);before=digest();
});
afterAll(()=>{if(dirname(resolve(root))!==resolve(tmpdir())||!basename(root).startsWith('team-ui-account-'))throw Error('UNSAFE_TEST_CLEANUP');rmSync(root,{recursive:true,force:true,maxRetries:10,retryDelay:50});});
function read(kind:string,extra={}){return JSON.parse(execFileSync(python,['-B',resolve('scripts/team_ui_readonly.py'),root,JSON.stringify({kind,trade_date:'2026-10-22',limit:50,offset:0,days:20,...extra})],{encoding:'utf8'}));}
test('dated lots and sell reservations stay scoped to their owner; same-day shares are never sellable',()=>{
 const result=read('account/positions');expect(result.rows).toHaveLength(2);
 expect(result.rows[0]).toMatchObject({plan_id:'owner:1',qty:10600,dated_sellable_qty:10500,t1_locked_qty:100,reserved_sell_qty:200,sellable_qty:10300,t1_locked:true});
 expect(result.rows[1]).toMatchObject({plan_id:'owner:2',qty:300,sellable_qty:300,t1_locked:false});
 expect(result.rows[0].price).toBeNull();expect(result.rows[0].unrealized_pnl).toBeNull();
 expect(result.ledger_scope).toMatch(/NOT_HISTORICAL/);expect(JSON.stringify(result)).not.toMatch(/SECRET|webhook|raw_response/);
});
test('lots show original buy day, do not fabricate next trading date and paginate without aggregation loss',()=>{
 const rows=read('account/lots').rows;expect(rows.find((x:{id:string})=>x.id==='new')).toMatchObject({open_date:'2026-10-22',sellable_qty:0,t1_locked:true,sellable_from:null});
 expect(read('account/lots',{limit:1}).next_offset).toBe(1);expect(read('account/positions',{limit:1,offset:1}).rows[0].plan_id).toBe('owner:2');
});
test('orders expose only allowlisted original values with provenance and no provider prose',()=>{
 const rows=read('account/orders').rows;expect(rows).toHaveLength(3);expect(rows[0].limit_price).toBe(18.2);expect(rows[0].source.sha256).toMatch(/^[a-f0-9]{64}$/);expect(JSON.stringify(rows)).not.toMatch(/SECRET|webhook|raw_response/);
});
test('new account routes share strict date alias validation and never accept conflicting dates',()=>{
 expect(parseTeamUiQuery('/account/positions',new URLSearchParams('date=2026-10-22'),'2026-10-21')?.trade_date).toBe('2026-10-22');
 for(const q of ['date=2026-10-22&date=2026-10-22','date=2026-02-30','date=2026-10-21&trade_date=2026-10-22'])expect(()=>parseTeamUiQuery('/account/lots',new URLSearchParams(q),'2026-10-22')).toThrow();
});
test('account projections preserve DB bytes and do not create WAL, sidecars or model artifacts',()=>{
 read('account/positions');read('account/orders');expect(digest()).toBe(before);expect(readdirSync(join(root,'state/team_v2'))).toEqual(['paper.db']);
});
