import {createServer,type Server} from 'node:http';
import {expect,test} from 'vitest';
import {createApp,type ApiDependencies} from '../server/api.js';
import {loadConfig} from '../server/config.js';

async function isolated(env:NodeJS.ProcessEnv,body:(base:string)=>Promise<void>){
 const config=loadConfig(env),server:Server=createServer(createApp({config,startedAt:Date.now(),dashboard:{overview:async()=>({existing_api:true})}} as unknown as ApiDependencies));
 await new Promise<void>(done=>server.listen(0,'127.0.0.1',done));const base=`http://127.0.0.1:${(server.address()as{port:number}).port}`;
 try{await body(base);}finally{server.closeAllConnections();await new Promise<void>(done=>server.close(()=>done()));}
}
const auth=(token:string)=>({headers:{Authorization:'Bearer '+token}});
test('dedicated Team config is trimmed, optional and never populates the old dashboard token',()=>{
 const config=loadConfig({LIANGJIAN_TEAM_UI_TOKEN:'  team-public-fixture  '});expect(config.teamUiToken).toBe('team-public-fixture');expect(config.dashboardToken).toBeNull();expect(config.teamUiEnabled).toBe(false);expect(loadConfig({LIANGJIAN_TEAM_UI_TOKEN:'  '}).teamUiToken).toBeNull();
});
test('dedicated Team token takes priority and cannot authenticate old API; existing token remains old API only',async()=>{
 await isolated({LIANGJIAN_TEAM_UI_ENABLED:'true',LIANGJIAN_TEAM_UI_TOKEN:'team-public-fixture',LIANGJIAN_DASHBOARD_TOKEN:'old-public-fixture'},async base=>{
  expect((await fetch(base+'/api/ui/session',auth('team-public-fixture'))).status).toBe(200);
  expect((await fetch(base+'/api/ui/session',auth('old-public-fixture'))).status).toBe(401);
  expect((await fetch(base+'/api/overview',auth('team-public-fixture'))).status).toBe(401);
  expect((await fetch(base+'/api/overview',auth('old-public-fixture'))).status).toBe(200);
  expect((await fetch(base+'/api/overview')).status).toBe(401);
  const login=await fetch(base+'/team/login',{method:'POST',body:new URLSearchParams({token:'team-public-fixture'}),redirect:'manual'});expect(login.status).toBe(303);const cookie=login.headers.get('set-cookie')!.split(';')[0]!;
  expect((await fetch(base+'/api/ui/session',{headers:{Cookie:cookie}})).status).toBe(200);expect((await fetch(base+'/api/overview',{headers:{Cookie:cookie}})).status).toBe(401);
  const page=await(await fetch(base+'/team/login')).text();expect(page).toContain('Team 专用令牌');expect(page).not.toContain('team-public-fixture');expect(page).not.toContain('old-public-fixture');
 });
});
test('no dedicated token retains legacy Team compatibility without changing existing API authentication',async()=>{
 await isolated({LIANGJIAN_TEAM_UI_ENABLED:'true',LIANGJIAN_DASHBOARD_TOKEN:'old-public-fixture'},async base=>{
  expect((await fetch(base+'/api/ui/session',auth('old-public-fixture'))).status).toBe(200);expect((await fetch(base+'/api/overview',auth('old-public-fixture'))).status).toBe(200);expect((await fetch(base+'/api/overview')).status).toBe(401);
 });
});
test('dedicated-only configuration does not silently enable authentication on previously tokenless old API',async()=>{
 await isolated({LIANGJIAN_TEAM_UI_ENABLED:'true',LIANGJIAN_TEAM_UI_TOKEN:'team-public-fixture'},async base=>{
  expect((await fetch(base+'/api/ui/session')).status).toBe(401);expect((await fetch(base+'/api/ui/session',auth('team-public-fixture'))).status).toBe(200);expect((await fetch(base+'/api/overview')).status).toBe(200);
 });
});
test('null Team and null legacy token fail closed for Team API and login',async()=>{
 await isolated({LIANGJIAN_TEAM_UI_ENABLED:'true'},async base=>{
  for(const token of [null,'anything'])expect((await fetch(base+'/api/ui/session',token?auth(token):undefined)).status).toBe(401);
  const login=await fetch(base+'/team/login',{method:'POST',body:new URLSearchParams({token:'anything'}),redirect:'manual'});expect(login.status).toBe(401);expect(login.headers.get('set-cookie')).toBeNull();expect((await fetch(base+'/api/overview')).status).toBe(200);
 });
});
test('disabled flag removes Team API and pages regardless of either configured token; old API remains unchanged',async()=>{
 await isolated({LIANGJIAN_TEAM_UI_ENABLED:'false',LIANGJIAN_TEAM_UI_TOKEN:'team-public-fixture',LIANGJIAN_DASHBOARD_TOKEN:'old-public-fixture'},async base=>{
  for(const path of ['/team','/team/login','/api/ui/session','/api/ui/dashboard'])for(const token of [null,'team-public-fixture','old-public-fixture'])expect((await fetch(base+path,token?auth(token):undefined)).status).toBe(404);
  expect((await fetch(base+'/api/overview',auth('old-public-fixture'))).status).toBe(200);expect((await fetch(base+'/api/overview',auth('team-public-fixture'))).status).toBe(401);
 });
});
