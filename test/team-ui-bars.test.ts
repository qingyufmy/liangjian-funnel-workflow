import {execFileSync} from 'node:child_process';
import {existsSync} from 'node:fs';
import {resolve} from 'node:path';
import {expect,test} from 'vitest';
const python=process.platform==='win32'?'python':'python3';
const packet=resolve('../.team-v2-model-switch-20261022/artifacts/postclose-core-20261022/a4-originals-0589.zjson');
const pinned='8577a6b2607a94ed4dfc1eb3f382968e75bba436b87eadfce3b76bcb798bf561';
function run(source:string,args:string[]=[]){return JSON.parse(execFileSync(python,['-B','-c',source,...args],{encoding:'utf8'}));}
test('display-only five-minute aggregation requires complete session buckets, valid values and exact cutoff/input binding',()=>{
 const result=run(String.raw`import sys,json,copy
from datetime import datetime,timedelta
sys.path.insert(0,'scripts')
from team_ui_bars import closed_five
origin=datetime.fromisoformat('2026-10-22T09:30:00+08:00')
bars=[dict(symbol='600201.SH',interval='1m',bar_end=(origin+timedelta(minutes=i)).isoformat(),open=10.,high=12.,low=9.,close=11.,volume=float(i),amount=float(i)*11.) for i in range(1,11)]
def read(rows=bars,**kw):return closed_five(json.dumps(rows),symbol='600201.SH',cutoff=kw.pop('cutoff','2026-10-22T09:40:00+08:00'),decision_as_of=kw.pop('decision_as_of','2026-10-22T09:41:00+08:00'),**kw)
checks=[]
out,sha=read();checks.append(len(out)==2 and out[0]['volume']==15 and out[0]['open']==10 and out[0]['close']==11)
checks.append(len(read(bars[:-1])[0])==1)
checks.append(len(read(cutoff='2026-10-22T09:39:00+08:00')[0])==1)
checks.append(read(expected_input_sha256=sha)[1]==sha)
def rejects(rows=bars,**kw):
 try:read(rows,**kw);return False
 except ValueError:return True
checks.append(rejects(expected_input_sha256='0'*64))
for key,value in [('symbol','OTHER'),('interval','5m'),('closed',False),('close',0),('high',9),('volume',-1),('amount',float('nan')),('bar_end','2026-10-22T09:42:00+08:00'),('source_id','X:RISK_ONLY'),('evidence_kind','SYNTHETIC_QUOTE')]:
 changed=copy.deepcopy(bars);changed[0][key]=value;checks.append(rejects(changed))
checks.append(rejects(bars+[bars[0]]))
checks.append(rejects(cutoff='2026-10-22T09:42:00+08:00'))
# A gap or lunch boundary cannot be filled by mixing minutes across sessions.
lunch=[{**bars[i],'bar_end':f'2026-10-22T{at}:00+08:00'} for i,at in enumerate(('11:29','11:30','13:01','13:02','13:03'))]
checks.append(read(lunch,cutoff='2026-10-22T13:05:00+08:00',decision_as_of='2026-10-22T13:06:00+08:00')[0]==[])
print(json.dumps(checks))`);
 expect(result.length).toBeGreaterThanOrEqual(18);expect(result.every(Boolean)).toBe(true);
});
test.skipIf(!existsSync(packet))('all 12 SHA-pinned actual 1m originals match execution aggregation exactly; UI read preserves SQLite bytes',()=>{
 const result=run(String.raw`import sys,json,tempfile,pathlib,hashlib
sys.path.insert(0,'scripts')
from team_ui_bars import closed_five
from team_ui_frozen import load_minute_packet,load_capture,load_supplement,build_fixture
from team_ui_readonly import project
packet=load_minute_packet(sys.argv[1],sys.argv[2]);body=packet['body']
sys.path.insert(0,str(pathlib.Path(sys.argv[3])/'src'))
from liangjian_funnel.runtime.strategies import aggregate_closed_bars
from datetime import datetime
events={(json.loads(e['payload_json'])['minute_snapshot_id'],json.loads(e['payload_json'])['symbol']):e for e in body['events']}
matches=[]
for snap in body['snapshots']:
 if snap['interval']!='1m':continue
 event=events[(snap['snapshot_id'],snap['symbol'])];execution=json.loads(event['payload_json'])['strategy']['execution_data'];cutoff=execution['market_cutoff']
 bars,digest=closed_five(snap['payload_original_utf8'],symbol=snap['symbol'],cutoff=cutoff,decision_as_of=snap['decision_as_of'],expected_input_sha256=execution['input_sha256'])
 original=[r for r in json.loads(snap['payload_original_utf8']) if datetime.fromisoformat(r['bar_end'])<=datetime.fromisoformat(cutoff)]
 expected=aggregate_closed_bars(original,as_of=datetime.fromisoformat(cutoff))['5m']
 matches.append(len(bars)==len(expected) and all(all(row[k]==ref['bar_end' if k=='at' else k] for k in ('at','open','high','low','close','volume','amount')) for row,ref in zip(bars,expected)))
capture,_=load_capture('artifacts/frontend-20261022/frozen-ledger-rows.json');supp=load_supplement('artifacts/frontend-20261022/frozen-runtime-supplement.json')
with tempfile.TemporaryDirectory(prefix='team-ui-frozen-bars-') as tmp:
 root=pathlib.Path(tmp);build_fixture(capture,root,supp,packet)
 before={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
 data=project(root,dict(kind='dashboard',trade_date='2026-10-22',limit=500,offset=0,days=20))
 plans=data['sections']['plans']['rows'];charts=[p for p in plans if p.get('bar_source',{}).get('basis')=='EXECUTION_AGGREGATED_5M_NOT_NATIVE']
 print(json.dumps(dict(matches=matches,chart_counts=[len(p['bars']) for p in charts],cutoffs=[p['bar_source']['cutoff'] for p in charts],readonly=all(hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()==sha for p,sha in before.items()),files_unchanged=len(before)==sum(p.is_file() for p in root.rglob('*')))))`,[packet,pinned,resolve('../.team-v2-model-switch-20261022')]);
 expect(result.matches).toHaveLength(12);expect(result.matches.every(Boolean)).toBe(true);expect(result.chart_counts).toHaveLength(3);expect(result.chart_counts.every((n:number)=>n>40&&n<48)).toBe(true);expect(result.cutoffs.every((s:string)=>s==='2026-10-22T14:54:00+08:00')).toBe(true);expect(result.readonly).toBe(true);expect(result.files_unchanged).toBe(true);
});
