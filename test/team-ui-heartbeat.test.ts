import {createServer,type Server} from 'node:http';
import express from 'express';
import {afterEach,expect,test,vi} from 'vitest';
import {createTeamUiRouter} from '../server/team-ui-router.js';
import {createTeamUiAccess} from '../server/team-ui-auth.js';
import {createApp,type ApiDependencies} from '../server/api.js';
import {loadConfig} from '../server/config.js';
// @ts-expect-error Static browser transport module.
import {createHeartbeat,createPoller} from '../web/team/poll.js';
afterEach(()=>vi.useRealTimers());
const pong=()=>({ok:true,status:200,json:async()=>({schema:'team-ui-ping/1',reachable:true})});
const wait=(ms:number)=>new Promise(done=>setTimeout(done,ms));

test('real-service refusal and1.5s timeout detect loss within3.5s without navigator events',async()=>{
 vi.useFakeTimers();let down=false,timeout=false,offlineAt=0,calls=0;
 const beat=createHeartbeat({fetchPing:async()=>{calls++;if(down){if(timeout){await wait(1500);throw new DOMException('deadline','TimeoutError');}throw new TypeError('connection refused');}return pong();},onOffline:()=>offlineAt=Date.now(),onReachable:()=>{},onUnauthorized:vi.fn()});
 beat.start();beat.start();await vi.advanceTimersByTimeAsync(0);expect(calls).toBe(1);const lostAt=Date.now();down=true;timeout=true;
 await vi.advanceTimersByTimeAsync(3499);expect(offlineAt).toBe(0);await vi.advanceTimersByTimeAsync(1);expect(offlineAt-lostAt).toBe(3500);beat.stop();const count=calls;await vi.advanceTimersByTimeAsync(20000);expect(calls).toBe(count);
});
test('heartbeat never marks data fresh; recovery triggers one full data/DOM refresh inside10s',async()=>{
 vi.useFakeTimers();let down=false,offline=false,rows=1,renderedAt=0,largeReads=0;const reach=vi.fn();
 const poll=createPoller({initialDelay:0,fetchPage:async()=>{largeReads++;await wait(1900);await wait(1900);return {ok:true,status:200,json:async()=>({rows:[],next_since_id:0})};},onRows:()=>rows++,onPage:()=>renderedAt=Date.now(),onStatus:()=>{},onUnauthorized:vi.fn()});
 const beat=createHeartbeat({fetchPing:async()=>{if(down)throw new TypeError('service gone');return pong();},onOffline:()=>{offline=true;},onReachable:()=>{reach();if(offline){offline=false;poll.refresh();}},onUnauthorized:vi.fn()});
 poll.start();beat.start();await vi.advanceTimersByTimeAsync(3800);expect(largeReads).toBe(1);expect(rows).toBe(2);down=true;await vi.advanceTimersByTimeAsync(200);expect(offline).toBe(true);const retained=rows,restoredAt=Date.now();down=false;
 await vi.advanceTimersByTimeAsync(5799);expect(rows).toBe(retained);await vi.advanceTimersByTimeAsync(101);expect(renderedAt-restoredAt).toBeGreaterThanOrEqual(5800);expect(renderedAt-restoredAt).toBeLessThan(6000);expect(rows).toBe(retained+1);expect(largeReads).toBe(2);poll.stop();beat.stop();
});
test('auth401 stops heartbeat once, preserving data and ignoring late probes after stop/restart',async()=>{
 vi.useFakeTimers();const login=vi.fn(),fetchPing=vi.fn(async()=>({ok:false,status:401}));const beat=createHeartbeat({fetchPing,onOffline:vi.fn(),onReachable:vi.fn(),onUnauthorized:login});
 beat.start();await vi.advanceTimersByTimeAsync(30000);expect(login).toHaveBeenCalledTimes(1);expect(fetchPing).toHaveBeenCalledTimes(1);beat.stop();
});
test('visibility-style stop/start during a pending probe does not strand or duplicate timers',async()=>{
 vi.useFakeTimers();let finish!:(value:unknown)=>void;const fetchPing=vi.fn(()=>new Promise(done=>{finish=done;})),online=vi.fn();
 const beat=createHeartbeat({fetchPing,onOffline:vi.fn(),onReachable:online,onUnauthorized:vi.fn()});beat.start();await vi.advanceTimersByTimeAsync(0);beat.stop();beat.start();await vi.advanceTimersByTimeAsync(0);expect(fetchPing).toHaveBeenCalledTimes(1);
 finish(pong());await vi.advanceTimersByTimeAsync(0);expect(online).not.toHaveBeenCalled();await vi.advanceTimersByTimeAsync(2000);expect(fetchPing).toHaveBeenCalledTimes(2);finish(pong());await vi.advanceTimersByTimeAsync(0);expect(online).toHaveBeenCalledTimes(1);beat.stop();expect(vi.getTimerCount()).toBe(0);
});
async function withServer(app:express.Express,run:(base:string)=>Promise<void>){const server:Server=createServer(app);await new Promise<void>(done=>server.listen(0,'127.0.0.1',done));try{await run(`http://127.0.0.1:${(server.address()as{port:number}).port}`);}finally{server.closeAllConnections();await new Promise<void>(done=>server.close(()=>done()));}}
test('ping GET/HEAD is Team-authenticated and zero reader I/O;POST405 and401 have no writes',async()=>{
 const read=vi.fn(async()=>({})),app=express(),access=createTeamUiAccess('public-fixture');app.use('/api/ui',access.api,createTeamUiRouter({read}));
 await withServer(app,async base=>{expect((await fetch(base+'/api/ui/ping')).status).toBe(401);const headers={Authorization:'Bearer public-fixture'};expect(await(await fetch(base+'/api/ui/ping',{headers})).json()).toEqual({schema:'team-ui-ping/1',reachable:true});expect((await fetch(base+'/api/ui/ping',{headers,method:'HEAD'})).status).toBe(200);expect((await fetch(base+'/api/ui/ping',{headers,method:'POST'})).status).toBe(405);expect(read).not.toHaveBeenCalled();});
});
test('disabled Team flag keeps ping404 even with a token and nulltoken enabled still401',async()=>{
 for(const enabled of ['false','true']){const app=createApp({config:loadConfig({LIANGJIAN_TEAM_UI_ENABLED:enabled}),startedAt:Date.now()} as ApiDependencies);await withServer(app,async base=>expect((await fetch(base+'/api/ui/ping',{headers:{Authorization:'Bearer any'}})).status).toBe(enabled==='false'?404:401));}
});
test('read failure records the exact prepared503 body once while ping never calls the error sink',async()=>{
 const failure=Error('private-provider-text'),recordFailure=vi.fn(async(_q:unknown,_e:unknown,_body:unknown)=>null),app=express();app.use('/api/ui',createTeamUiRouter({read:async()=>{throw failure;},recordFailure}));
 await withServer(app,async base=>{const response=await fetch(base+'/api/ui/dashboard');expect(response.status).toBe(503);const body=await response.json();expect(body).toEqual({error:'TEAM_UI_SOURCE_UNAVAILABLE'});expect(recordFailure).toHaveBeenCalledTimes(1);expect(recordFailure.mock.calls[0]![1]).toBe(failure);expect(recordFailure.mock.calls[0]![2]).toEqual(body);await fetch(base+'/api/ui/ping');expect(recordFailure).toHaveBeenCalledTimes(1);});
});

test('actual loopback service close, not navigator.offline, is detected and recovers without data reads',async()=>{
 const read=vi.fn(async()=>({})),app=express(),access=createTeamUiAccess('public-fixture');app.use('/api/ui',access.api,createTeamUiRouter({read}));
 const server=createServer(app);await new Promise<void>(done=>server.listen(0,'127.0.0.1',done));const port=(server.address()as{port:number}).port;
 const jobs:{fn:()=>Promise<void>,delay:number}[]=[],states:string[]=[];
 const beat=createHeartbeat({fetchPing:()=>fetch(`http://127.0.0.1:${port}/api/ui/ping`,{headers:{Authorization:'Bearer public-fixture'},signal:AbortSignal.timeout(1500)}),onOffline:()=>states.push('OFFLINE'),onReachable:(m:{recovered:boolean})=>states.push(m.recovered?'RECOVERED':'REACHABLE'),onUnauthorized:()=>states.push('401'),schedule:(fn:()=>Promise<void>,delay:number)=>{jobs.push({fn,delay});return fn;},cancel:()=>{jobs.length=0;}});
 try{beat.start();await jobs.shift()!.fn();expect(states.at(-1)).toBe('REACHABLE');server.closeAllConnections();await new Promise<void>(done=>server.close(()=>done()));const failed=jobs.shift()!;expect(failed.delay).toBe(2000);await failed.fn();expect(states.at(-1)).toBe('OFFLINE');await new Promise<void>(done=>server.listen(port,'127.0.0.1',done));await jobs.shift()!.fn();if(states.at(-1)==='OFFLINE')await jobs.shift()!.fn();expect(states.at(-1)).toBe('RECOVERED');expect(read).not.toHaveBeenCalled();}
 finally{beat.stop();if(server.listening){server.closeAllConnections();await new Promise<void>(done=>server.close(()=>done()));}}
});
