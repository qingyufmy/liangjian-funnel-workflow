import {execFileSync} from 'node:child_process';
import {mkdtempSync,readFileSync,readdirSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join,basename,dirname} from 'node:path';
import {createHash} from 'node:crypto';
import {beforeAll,afterAll,test,expect} from 'vitest';
import {parseTeamUiQuery} from '../server/team-ui-readonly.js';
const python=process.platform==='win32'?'python':'python3';let root:string,before:string;
const sha=(p:string)=>createHash('sha256').update(readFileSync(p)).digest('hex');
beforeAll(()=>{
  root=mkdtempSync(join(tmpdir(),'team-ui-events-test-'));
  execFileSync(python,['-B','-c',`import sqlite3,sys
from pathlib import Path
p=Path(sys.argv[1])/'state/team_v2';p.mkdir(parents=True)
c=sqlite3.connect(p/'model.db');c.execute('CREATE TABLE model_calls(call_id,role,session,captured_at,latency_ms,provider_attempts,validated,output_hash)')
for i in range(5):c.execute('INSERT INTO model_calls VALUES(?,?,?,?,?,?,?,?)',('call:'+str(i),'A4','INTRADAY','2026-10-21T10:00:00+08:00' if i<4 else '2026-10-22T10:00:00+08:00',123,1,i%2,'a'*64))
c.commit();c.close()`,root]);
  before=sha(join(root,'state/team_v2/model.db'));
});
afterAll(()=>{if(dirname(resolve(root))!==resolve(tmpdir())||!basename(root).startsWith('team-ui-events-test-'))throw Error('UNSAFE_TEST_CLEANUP');rmSync(root,{recursive:true,force:true,maxRetries:10,retryDelay:50});});
const read=(since:number)=>JSON.parse(execFileSync(python,['-B',resolve('scripts/team_ui_readonly.py'),root,JSON.stringify({kind:'events',trade_date:'2026-10-21',limit:2,offset:0,since_id:since})],{encoding:'utf8'}));
test('since_id advances over original rows, excludes other day and never repeats old rows',()=>{
  const first=read(0);expect(first.rows.map((x:{id:string})=>x.id)).toEqual(['call:0','call:1']);expect(first.next_since_id).toBe(2);expect(first.has_more).toBe(true);
  const next=read(2);expect(next.rows.map((x:{id:string})=>x.id)).toEqual(['call:2','call:3']);expect(next.next_since_id).toBe(4);expect(next.has_more).toBe(false);
  expect(read(4).rows).toEqual([]);expect(read(4).next_since_id).toBe(4);
  expect(first.coverage_kind).toContain('NOT_PLAN_TRANSITIONS');expect(first.rows[0].provenance.sha256).toMatch(/^[a-f0-9]{64}$/);
});
test('cursor rejected before IO, reads do not create files or modify source',()=>{
  for(const value of ['-1','NaN','9007199254740992','1&since_id=2'])expect(()=>parseTeamUiQuery('/events',new URLSearchParams('since_id='+value),'2026-10-21')).toThrow();
  expect(()=>parseTeamUiQuery('/events',new URLSearchParams('offset=1'),'2026-10-21')).toThrow();
  read(0);expect(sha(join(root,'state/team_v2/model.db'))).toBe(before);expect(readdirSync(join(root,'state/team_v2'))).toEqual(['model.db']);
});
