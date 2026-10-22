import {execFileSync} from 'node:child_process';
import {existsSync} from 'node:fs';
import {resolve} from 'node:path';
import {expect,test} from 'vitest';
const python=process.platform==='win32'?'python':'python3';
const run=(code:string)=>JSON.parse(execFileSync(python,['-B','-c',code],{encoding:'utf8'}));
test('condition source preserves time lists/frozen plans, no invented timestamp, future provenance stays invalid',()=>{
 const r=run(String.raw`import sys,json
sys.path.insert(0,'scripts')
from team_ui_plan_detail import condition_source,compact
at='2026-10-22T15:00:00+08:00'
values=['FROZEN_PLAN',['2026-10-21T00:00:00+08:00','2026-10-22T14:55:00+08:00'],[],None,'2026-10-22T15:01:00+08:00',['2026-10-22T14:55:00+08:00','2026-10-22T15:01:00+08:00'],'2026-10-22T14:55:00']
print(json.dumps(dict(sources=[condition_source(v,at) for v in values],value=compact([17.4,17.39,17.17]),threshold=compact(dict(level=17.078,pullback_mean=169175.,rise_mean=123066.66666666667)),raw=values)))`);
 expect(r.sources[0]).toEqual({at:null,source_kind:'FROZEN_PLAN',source_times:[],source_valid:true});expect(r.sources[1]).toEqual({at:null,source_kind:'TIMESTAMP_LIST',source_times:r.raw[1],source_valid:true});expect(r.sources[2].at).toBeNull();expect(r.sources[3].at).toBeNull();expect(r.sources[3].source_valid).toBeNull();for(const s of r.sources.slice(4))expect(s.source_valid).toBe(false);expect(r.value).toEqual([17.4,17.39,17.17]);expect(r.threshold).toEqual({level:17.078,pullback_mean:169175,rise_mean:123066.66666666667});
});
test.skipIf(!existsSync(resolve('artifacts/frontend-20261022/frozen-ledger-rows.json')))('real three-plan required rows and original judgments survive display projection without narrowing condition domain',()=>{
 const result=run(String.raw`import sys,json,tempfile,pathlib
sys.path.insert(0,'scripts')
from team_ui_frozen import load_capture,load_supplement,build_fixture
from team_ui_readonly import project
capture,_=load_capture('artifacts/frontend-20261022/frozen-ledger-rows.json')
with tempfile.TemporaryDirectory(prefix='team-ui-condition-') as tmp:
 root=pathlib.Path(tmp);build_fixture(capture,root,load_supplement('artifacts/frontend-20261022/frozen-runtime-supplement.json'))
 rows=project(root,dict(kind='dashboard',trade_date='2026-10-22',limit=500,offset=0,days=20))['sections']['plans']['rows']
 output=[]
 for row in rows:
  source=next(json.loads(e['payload_json'])['strategy']['entry_confirmation'] for e in sorted(capture['tables']['state/workflow.sqlite3:monitor_events'],key=lambda e:e['minute_end'],reverse=True) if json.loads(e['payload_json']).get('plan_id')==row['id'] and json.loads(e['payload_json']).get('strategy',{}).get('entry_confirmation',{}).get('conditions'))
  output.append(dict(code=row['code'],required=[c['code'] for c in row['conditions']],expected_required=source['required_conditions'],statuses=[c['status'] for c in row['conditions']],expected_statuses=[source['conditions'][k]['status'] for k in source['required_conditions']],met=sum(c['status']=='MET' and c['source_valid'] is not False for c in row['conditions']),count=len(row['conditions'])))
 print(json.dumps(output))`);
 expect(result).toHaveLength(3);const counts=Object.fromEntries(result.map((r:{code:string,met:number,count:number})=>[r.code,[r.met,r.count]]));expect(counts).toEqual({'600201.SH':[3,10],'002354.SZ':[1,7],'300556.SZ':[5,7]});for(const row of result){expect(row.required).toEqual(row.expected_required);expect(row.statuses).toEqual(row.expected_statuses);}
});
