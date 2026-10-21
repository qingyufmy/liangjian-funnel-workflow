/** Local synthetic acceptance probe only. Never accepts a production root. */
import {execFile} from 'node:child_process';
import {createHash} from 'node:crypto';
import {mkdtemp,mkdir,copyFile,readFile,readdir,rm} from 'node:fs/promises';
import {createServer} from 'node:http';
import {tmpdir} from 'node:os';
import {basename,dirname,join,resolve} from 'node:path';
import {monitorEventLoopDelay,performance} from 'node:perf_hooks';
import {setTimeout as sleep} from 'node:timers/promises';
import {promisify} from 'node:util';
import express from 'express';
import {dashboardAuth} from '../server/auth.js';
import {TeamUiReadOnly} from '../server/team-ui-readonly.js';
import {createTeamUiRouter} from '../server/team-ui-router.js';

const run=promisify(execFile),python=process.platform==='win32'?'python':'python3';
const root=await mkdtemp(join(tmpdir(),'team-ui-benchmark-'));
const hash=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
const server=createServer();
try{
  await mkdir(join(root,'scripts'));await mkdir(join(root,'state'));
  const script=resolve('scripts/team_ui_readonly.py');await copyFile(script,join(root,'scripts/team_ui_readonly.py'));
  await run(python,['-B','-c',`import sqlite3,json,sys
from pathlib import Path
c=sqlite3.connect(Path(sys.argv[1])/'state/workflow.sqlite3')
c.execute('CREATE TABLE execution_plans(plan_id,symbol,status,valid_from,expires_at,updated_at,payload_json)')
for i in range(160):
 c.execute('INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?)',('fixture:'+str(i),'000001.SZ','ACTIVE_TODAY','2026-10-21T09:26:00+08:00',None,'2026-10-21T10:00:00+08:00',json.dumps({'name':'Fixture','strategy_profile':'TREND_MA5','trigger_low':'10','trigger_high':'10.2','stop_level':'9.8','risk_unit':'PROBE'})))
c.commit();c.close()
`,root],{windowsHide:true});
  const db=join(root,'state/workflow.sqlite3');const before=hash(await readFile(db));
  const filesBefore=await readdir(join(root,'state'));
  const reader=new TeamUiReadOnly(root,python);
  const app=express();app.use('/api/ui',dashboardAuth('synthetic-only'),createTeamUiRouter(reader,()=>'2026-10-21'));
  server.on('request',app);await new Promise<void>(done=>server.listen(0,'127.0.0.1',done));
  const base=`http://127.0.0.1:${(server.address() as {port:number}).port}/api/ui/day/2026-10-21/plans`;
  const lag=monitorEventLoopDelay({resolution:10});lag.enable();await sleep(1_000);
  const baseline={p50_ms:lag.percentile(50)/1e6,p95_ms:lag.percentile(95)/1e6,max_ms:lag.max/1e6};
  lag.reset();const start=performance.now();const latencies:number[]=[];const statuses:Record<string,number>={};
  await Promise.all(Array.from({length:8},async(_,client)=>{
    for(let i=0;i<80;i++){
      const begun=performance.now();
      const response=await fetch(`${base}?limit=50&offset=${(client+i)%24}`,{headers:{Authorization:'Bearer synthetic-only'}});
      const body=await response.json();
      if(response.status===200&&(body.status!=='AVAILABLE'||!Array.isArray(body.rows)))throw Error('PROJECTION_INVALID');
      if(response.status!==200&&(response.status!==503||body.error!=='TEAM_UI_BUSY'))throw Error('UNEXPECTED_RESPONSE');
      statuses[String(response.status)]=(statuses[String(response.status)]??0)+1;
      latencies.push(performance.now()-begun);await sleep(5);
    }
  }));
  const elapsed=performance.now()-start;
  const loaded={p50_ms:lag.percentile(50)/1e6,p95_ms:lag.percentile(95)/1e6,max_ms:lag.max/1e6};lag.disable();
  const after=hash(await readFile(db));const filesAfter=await readdir(join(root,'state'));
  latencies.sort((a,b)=>a-b);
  process.stdout.write(JSON.stringify({
    scope:'LOCAL_SYNTHETIC_SQLITE_HTTP_NODE_ONLY_NOT_VM_OR_LIVE_TRADING_ACCEPTANCE',
    actual_at:new Date().toISOString(),requests:latencies.length,clients:8,load_elapsed_ms:elapsed,statuses,
    response_latency:{p50_ms:latencies[Math.floor(latencies.length/2)],p95_ms:latencies[Math.ceil(.95*latencies.length)-1],max_ms:latencies.at(-1)},
    node_event_loop: {baseline,loaded,p95_delta_ms:loaded.p95_ms-baseline.p95_ms},
    worker:reader.diagnostics(),readonly:{database_sha_before:before,database_sha_after:after,unchanged:before===after,files_before:filesBefore,files_after:filesAfter,no_new_files:JSON.stringify(filesBefore)===JSON.stringify(filesAfter)},
    reader_script_sha256:hash(await readFile(script)),production_connections:0,models:0,notifications:0,
    limits:{child_timeout_ms:450,concurrency:2,cache_ttl_ms:2000,cache_entries:64},
    missing_gate:'VM current Node event-loop/latency and VM schema/file acceptance deferred until approved post-close window'
  })+'\n');
}finally{
  server.closeAllConnections();if(server.listening)await new Promise<void>(done=>server.close(()=>done()));
  // Only this invocation's newly-created synthetic fixture; no project/VM data.
  if(dirname(resolve(root))!==resolve(tmpdir())||!basename(root).startsWith('team-ui-benchmark-'))throw Error('UNSAFE_TEMP_CLEANUP');
  await rm(root,{recursive:true,force:true});
}
