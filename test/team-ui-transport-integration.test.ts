import {createServer,type Server} from 'node:http';
import express from 'express';
import {expect,test} from 'vitest';
import {createTeamUiRouter} from '../server/team-ui-router.js';
import {createTeamUiAccess} from '../server/team-ui-auth.js';
import {TeamUiBusyError} from '../server/team-ui-readonly.js';
// @ts-expect-error Browser transport reused without a DOM.
import {createPoller,freshness} from '../web/team/poll.js';
type Job={fn:()=>Promise<void>,delay:number};
function scheduler(){let clock=0;const jobs:Job[]=[];return {jobs,now:()=>clock,schedule:(fn:()=>Promise<void>,delay:number)=>{jobs.push({fn,delay});return fn;},cancel:()=>{jobs.length=0;},run:async()=>{const job=jobs.shift()!;clock+=job.delay;await job.fn();return job.delay;}};}
async function listen(server:Server,port=0){await new Promise<void>(done=>server.listen(port,'127.0.0.1',done));return(server.address()as {port:number}).port;}
async function close(server:Server){server.closeAllConnections();await new Promise<void>(done=>server.close(()=>done()));}
function app(read:()=>Promise<unknown>){const app=express(),auth=createTeamUiAccess('transport-public-fixture');app.use(express.urlencoded({extended:false}));app.post('/team/login',auth.login);app.use('/api/ui',auth.api,createTeamUiRouter({read},()=> '2026-10-22'));return app;}

test('real loopback refusal is offline; same-Node HTTP recovery retries within5s plus response time',async()=>{
 const server=createServer(app(async()=>({rows:[],next_since_id:0}))),port=await listen(server),base=`http://127.0.0.1:${port}`;
 const queue=scheduler(),states:string[]=[];let last:number|null=null;
 const poll=createPoller({fetchPage:()=>fetch(base+'/api/ui/events',{headers:{Authorization:'Bearer transport-public-fixture'}}),onRows:()=>{},onStatus:(_s:string,m:{state:string,received_at?:number})=>{states.push(m.state);if(m.received_at!=null)last=m.received_at;},onUnauthorized:()=>{throw Error('unexpected 401');},schedule:queue.schedule,cancel:queue.cancel,now:queue.now});
 try{
  poll.start();expect(await queue.run()).toBe(10000);expect(states).toEqual(['CURRENT']);
  await close(server);expect(await queue.run()).toBe(10000);expect(states.at(-1)).toBe('OFFLINE');expect(queue.jobs[0]!.delay).toBe(5000);
  expect(freshness(last,(last??0)+30000)).toBe('STALE');await listen(server,port);
  expect(await queue.run()).toBe(5000);expect(states.at(-1)).toBe('CURRENT');
 }finally{poll.stop();if(server.listening)await close(server);}
});

test('real HTTP503 keeps cursor/data and backs off20/40/60; recreating auth key returns401 and stops',async()=>{
 let busy=false;const server=createServer(app(async()=>{if(busy)throw new TeamUiBusyError('TEAM_UI_BUSY');return {rows:[{cursor_id:4}],next_since_id:4};}));
 const port=await listen(server),base=`http://127.0.0.1:${port}`,queue=scheduler(),received:unknown[]=[],states:string[]=[];let unauthorized=0;
 const login=await fetch(base+'/team/login',{method:'POST',body:new URLSearchParams({token:'transport-public-fixture'}),redirect:'manual'}),cookie=login.headers.get('set-cookie')!.split(';')[0]!;
 const poll=createPoller({fetchPage:()=>fetch(base+'/api/ui/events',{headers:{Cookie:cookie}}),onRows:(r:unknown[])=>received.push(...r),onStatus:(_s:string,m:{state:string})=>states.push(m.state),onUnauthorized:()=>unauthorized++,schedule:queue.schedule,cancel:queue.cancel,now:queue.now});let restarted:Server|null=null;
 try{
  poll.start();await queue.run();expect(received).toHaveLength(1);busy=true;
  await queue.run();expect(queue.jobs[0]!.delay).toBe(20000);await queue.run();expect(queue.jobs[0]!.delay).toBe(40000);await queue.run();expect(queue.jobs[0]!.delay).toBe(60000);expect(poll.cursor()).toBe(4);expect(received).toHaveLength(1);
  await close(server);restarted=createServer(app(async()=>({rows:[],next_since_id:4})));await listen(restarted,port);
  // undici may first reuse the just-closed socket. That real network refusal
  // must preserve data and retry, not be mistaken for authentication success.
  await queue.run();if(!unauthorized){expect(states.at(-1)).toBe('OFFLINE');expect(queue.jobs[0]!.delay).toBe(5000);await queue.run();}
  expect(unauthorized).toBe(1);expect(queue.jobs).toEqual([]);expect(received).toHaveLength(1);
 }finally{poll.stop();if(server.listening)await close(server);if(restarted?.listening)await close(restarted);}
});
