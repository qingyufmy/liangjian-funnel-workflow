/** Loopback QA only. Uses actual Node API/router/auth and synthetic throwaway DBs.
 * Never accepts a production root or starts runner/scheduler/business workflows. */
import {createServer} from 'node:http';
import {mkdtemp,mkdir,copyFile,cp,readFile,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,dirname,basename,sep} from 'node:path';
import {execFileSync} from 'node:child_process';
import {watch} from 'node:fs';
import {createApp,type ApiDependencies} from '../server/api.js';
import {loadConfig} from '../server/config.js';

const root=await mkdtemp(join(tmpdir(),'team-ui-synthetic-'));
await mkdir(join(root,'scripts'));await copyFile(resolve('scripts/team_ui_readonly.py'),join(root,'scripts/team_ui_readonly.py'));
await mkdir(join(root,'web'));await cp(resolve('web/team'),join(root,'web/team'),{recursive:true});
const html=join(root,'web/team/index.html');
await writeFile(html,(await readFile(html,'utf8')).replace('<body>','<body><p style="padding:8px;background:#fdf3d7;color:#7a5200">本机合成验证 · 不是生产计划、事件或账户</p>'));
const python=process.platform==='win32'?'python':'python3';
execFileSync(python,['-B','-c',`import sqlite3,json,sys
from pathlib import Path
p=Path(sys.argv[1]);(p/'state/team_v2').mkdir(parents=True)
w=sqlite3.connect(p/'state/workflow.sqlite3');w.execute('CREATE TABLE execution_plans(plan_id,symbol,status,valid_from,expires_at,updated_at,payload_json)')
for i in range(3):w.execute('INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?)',('synthetic:'+str(i),'000000.TEST','ACTIVE_TODAY','2026-10-21T09:26:00+08:00',None,'2026-10-21T10:00:00+08:00',json.dumps({'name':'合成验证股票'+str(i),'trigger_low':10,'trigger_high':10.2,'stop_level':9.8,'strategy_profile':'TREND_MA5','risk_unit':'PROBE'})))
w.commit();w.close()
m=sqlite3.connect(p/'state/team_v2/model.db');m.execute('CREATE TABLE model_calls(call_id,role,session,captured_at,latency_ms,provider_attempts,validated,output_hash)')
for i in range(3):m.execute('INSERT INTO model_calls VALUES(?,?,?,?,?,?,?,?)',('synthetic-call:'+str(i),'A4','INTRADAY','2026-10-21T10:00:00+08:00',100,1,1,'a'*64))
m.commit();m.close()`,root]);
const config=loadConfig({LIANGJIAN_TEAM_UI_ENABLED:'true',LIANGJIAN_DASHBOARD_TOKEN:'team-ui-synthetic-only',LIANGJIAN_PYTHON_BIN:python},root);
const server=createServer(createApp({config:{...config,pythonBin:python},startedAt:Date.now()}as ApiDependencies));
const sourceWeb=resolve('web/team');
const watcher=watch(sourceWeb,{recursive:true},(_event,filename)=>{
  if(!filename)return;
  const source=resolve(sourceWeb,filename),target=resolve(root,'web/team',filename);
  if(!source.startsWith(sourceWeb+sep)||!target.startsWith(resolve(root,'web/team')+sep))return;
  void (async()=>{try{await mkdir(dirname(target),{recursive:true});if(filename==='index.html')await writeFile(target,(await readFile(source,'utf8')).replace('<body>','<body><p style="padding:8px;background:#fdf3d7;color:#7a5200">本机合成验证 · 不是生产计划、事件或账户</p>'));else await copyFile(source,target);}catch{/* Deleted/partial editor files are not a business error. */}})();
});
server.listen(8788,'127.0.0.1',()=>console.log('SYNTHETIC ONLY http://127.0.0.1:8788/team; login token is the public fixture value team-ui-synthetic-only'));
async function stop(){watcher.close();server.closeAllConnections();await new Promise<void>(done=>server.close(()=>done()));if(dirname(resolve(root))!==resolve(tmpdir())||!basename(root).startsWith('team-ui-synthetic-'))throw Error('UNSAFE_QA_CLEANUP');await rm(root,{recursive:true,force:true,maxRetries:10,retryDelay:50});}
process.once('SIGINT',()=>{void stop();});process.once('SIGTERM',()=>{void stop();});
