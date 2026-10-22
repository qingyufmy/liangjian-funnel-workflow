import {createServer,type Server} from 'node:http';
import express from 'express';
import {beforeAll,afterAll,test,expect} from 'vitest';
import {createTeamUiAccess} from '../server/team-ui-auth.js';

let server:Server,base:string,time=Date.now();
beforeAll(async()=>{
  const app=express(),access=createTeamUiAccess('synthetic-internal-token',()=>time);
  app.get('/team/login',access.loginPage);
  app.post('/team/login',express.urlencoded({extended:false,limit:'1kb'}),access.login);
  app.get('/team',access.page,(_q,r)=>r.send('PRIVATE_UI'));
  app.get('/api/ui/example',access.api,(_q,r)=>r.json({private_field:1}));
  server=createServer(app);await new Promise<void>(done=>server.listen(0,'127.0.0.1',done));
  base=`http://127.0.0.1:${(server.address()as{port:number}).port}`;
});
afterAll(async()=>{server.closeAllConnections();await new Promise<void>(done=>server.close(()=>done()));});

test('unlogged page redirects to login; API gives only401 without schema',async()=>{
  const page=await fetch(base+'/team',{redirect:'manual'});expect(page.status).toBe(303);expect(page.headers.get('location')).toBe('/team/login');
  const api=await fetch(base+'/api/ui/example');expect(api.status).toBe(401);expect(await api.text()).toBe('{"error":"UNAUTHORIZED"}');
  const login=await fetch(base+'/team/login');expect(login.status).toBe(200);expect(await login.text()).toContain('type="password"');
});
test('wrong or absent token does not issue session',async()=>{
  const response=await fetch(base+'/team/login',{method:'POST',body:new URLSearchParams({token:'wrong'}),redirect:'manual'});
  expect(response.status).toBe(401);expect(response.headers.get('set-cookie')).toBeNull();
});
test('login issues bounded signed HttpOnly cookie, never stores/echoes original token',async()=>{
  const response=await fetch(base+'/team/login',{method:'POST',body:new URLSearchParams({token:'synthetic-internal-token'}),redirect:'manual'});
  expect(response.status).toBe(303);expect(response.headers.get('location')).toBe('/team');
  const cookie=response.headers.get('set-cookie')!;expect(cookie).toContain('HttpOnly');expect(cookie).toContain('SameSite=Strict');expect(cookie).not.toContain('synthetic-internal-token');
  const header=cookie.split(';')[0]!;
  expect((await fetch(base+'/api/ui/example',{headers:{Cookie:header}})).status).toBe(200);
  expect((await fetch(base+'/team',{headers:{Cookie:header}})).status).toBe(200);
  expect((await fetch(base+'/api/ui/example',{headers:{Cookie:header.replace(/.$/,'z')}})).status).toBe(401);
  time+=8*60*60*1000+1;
  expect((await fetch(base+'/api/ui/example',{headers:{Cookie:header}})).status).toBe(401);
});
test('existing internal bearer access remains supported',async()=>{
  expect((await fetch(base+'/api/ui/example',{headers:{Authorization:'Bearer synthetic-internal-token'}})).status).toBe(200);
});
