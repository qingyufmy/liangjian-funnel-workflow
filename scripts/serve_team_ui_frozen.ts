/** Explicit local frozen replay through the real auth/router/read worker. No VM. */
import {createServer} from 'node:http';
import {mkdtemp,mkdir,copyFile,cp,readFile,writeFile,rm,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,dirname,basename} from 'node:path';
import {execFileSync} from 'node:child_process';
import {watch} from 'node:fs';
import {createApp,type ApiDependencies} from '../server/api.js';
import {loadConfig} from '../server/config.js';
const root=await mkdtemp(join(tmpdir(),'team-ui-frozen-qa-'));
const python=process.platform==='win32'?'python':'python3';
const packetIndex=process.argv.indexOf('--minute-packet'),shaIndex=process.argv.indexOf('--minute-sha');
const packetArgs=packetIndex>=0&&shaIndex>=0?['--minute-packet',resolve(process.argv[packetIndex+1]),'--minute-sha',process.argv[shaIndex+1]]:[];
const metadataIndex=process.argv.indexOf('--health-metadata'),receiptIndex=process.argv.indexOf('--health-receipt');
if(receiptIndex>=0&&metadataIndex<0)throw Error('QA_HEALTH_RECEIPT_REQUIRES_SOURCE_METADATA');
execFileSync(python,['-B',resolve('scripts/team_ui_frozen.py'),resolve('artifacts/frontend-20261022/frozen-ledger-rows.json'),'--fixture-root',root,...packetArgs],{stdio:'inherit'});
if(metadataIndex>=0)execFileSync(python,['-B',resolve('scripts/team_ui_health_fixture.py'),root,resolve(process.argv[metadataIndex+1]),...(receiptIndex>=0?['--ops-receipt',resolve(process.argv[receiptIndex+1])]:[])],{stdio:'inherit'});
await mkdir(join(root,'scripts'));
async function syncPython(){for(const name of await readdir(resolve('scripts')))if(/^team_ui_[a-z_]+\.py$/.test(name))await copyFile(resolve('scripts',name),join(root,'scripts',name));}
await syncPython();
// Catch incomplete QA helper copies before exposing a half-working page. This
// is a read of the isolated fixture, not a production warmup or a business run.
const smoke=JSON.parse(execFileSync(python,['-B',join(root,'scripts/team_ui_readonly.py'),root,JSON.stringify({kind:'dashboard',trade_date:'2026-10-22',limit:50,offset:0,days:20})],{encoding:'utf8',maxBuffer:512*1024}));
if(smoke.status!=='AVAILABLE'||!smoke.account||smoke.sections?.plans?.status!=='AVAILABLE')throw Error('LOCAL_QA_INCOMPLETE_DASHBOARD');
if(metadataIndex>=0&&(smoke.health_history?.monitor?.day_rows!==712||smoke.metrics?.team_effective_admissions!==1))throw Error('LOCAL_QA_HEALTH_SOURCE_INCOMPLETE');
await mkdir(join(root,'web'));await cp(resolve('web/team'),join(root,'web/team'),{recursive:true});
const html=join(root,'web/team/index.html');
const banner=`<body><p style="padding:10px 24px;background:#292111;color:#fcd34d;font-size:14px">10-22 已核SHA冻结原件 · 非实时 · 16:35账本小表、${metadataIndex>=0?'全日712条监控小元数据/真实晨会通知/有界历史':'15条监控尾段'}${packetArgs.length?'及4个时点原件/执行聚合5m':''}，不是完整全天原始轨迹</p>`;
await writeFile(html,(await readFile(html,'utf8')).replace('<body>',banner));
const config=loadConfig({LIANGJIAN_TEAM_UI_ENABLED:'true',LIANGJIAN_TEAM_UI_TOKEN:'team-ui-frozen-local-only',LIANGJIAN_PYTHON_BIN:python},root);
const server=createServer(createApp({config,startedAt:Date.now()} as ApiDependencies));
let syncTimer:ReturnType<typeof setTimeout>|null=null,syncing=false,syncAgain=false,stopping=false;
async function syncWeb(){
 if(stopping)return;if(syncing){syncAgain=true;return;}syncing=true;
 try{do{syncAgain=false;await cp(resolve('web/team'),join(root,'web/team'),{recursive:true});await writeFile(html,(await readFile(html,'utf8')).replace('<body>',banner));}while(syncAgain&&!stopping);}catch{console.error('LOCAL_QA_SYNC_INCOMPLETE_RETRY_AFTER_SAVE');}finally{syncing=false;}
}
const watchers=[watch(resolve('web/team'),{recursive:true},()=>{if(syncTimer)clearTimeout(syncTimer);syncTimer=setTimeout(()=>void syncWeb(),120);}),watch(resolve('scripts'),(_event,name)=>{if(name&&/^team_ui_[a-z_]+\.py$/.test(name))void syncPython().catch(()=>console.error('LOCAL_QA_PY_SYNC_INCOMPLETE'));})];
server.listen(8790,'127.0.0.1',()=>console.log('FROZEN LOCAL ONLY http://127.0.0.1:8790/team public fixture login: team-ui-frozen-local-only'));
async function stop(){if(stopping)return;stopping=true;if(syncTimer)clearTimeout(syncTimer);for(const w of watchers)w.close();server.closeAllConnections();await new Promise<void>(done=>server.close(()=>done()));if(dirname(resolve(root))!==resolve(tmpdir())||!basename(root).startsWith('team-ui-frozen-qa-'))throw Error('UNSAFE_QA_CLEANUP');await rm(root,{recursive:true,force:true,maxRetries:10,retryDelay:50});}
process.once('SIGINT',()=>void stop());process.once('SIGTERM',()=>void stop());
process.stdin.on('data',data=>{if(data.toString().trim()==='stop')void stop().then(()=>process.exit(0));});
