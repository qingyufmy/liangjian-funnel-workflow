import { createServer, type Server } from 'node:http';
import express from 'express';
import { afterAll, beforeAll, expect, test, vi } from 'vitest';
import { dashboardAuth } from '../server/auth.js';
import { createTeamUiRouter } from '../server/team-ui-router.js';
import { createApp, type ApiDependencies } from '../server/api.js';
import { loadConfig } from '../server/config.js';

let server: Server, base: string;
const read = vi.fn(async (query: unknown) => ({schema:'team-ui-readonly/1',query,status:'MISSING_SOURCE',rows:null}));
beforeAll(async () => {
  const app = express(); app.use('/api/ui', dashboardAuth('fixture-token'), createTeamUiRouter({read}, ()=>'2026-10-21'));
  server = createServer(app); await new Promise<void>(done => server.listen(0,'127.0.0.1',done));
  base = `http://127.0.0.1:${(server.address() as {port:number}).port}/api/ui`;
});
afterAll(async () => {server.closeAllConnections();await new Promise<void>(done=>server.close(()=>done()));});
const auth = {Authorization:'Bearer fixture-token'};
test('internal auth rejects access before any reader call',async()=>{
  const count=read.mock.calls.length;
  expect((await fetch(base+'/day/2026-10-21/plans')).status).toBe(401);
  expect(read.mock.calls.length).toBe(count);
});
test('all nine read signatures keep today/date and missing-source truth',async()=>{
  for(const path of ['/day/2026-10-21/tasks','/day/2026-10-21/feed','/day/2026-10-21/plans','/day/2026-10-21/fills','/role/tr/today','/role/tr/stats','/review/scoreboard','/review/lessons','/settings']){
    const response=await fetch(base+path,{headers:auth});expect(response.status).toBe(200);
    const body=await response.json();expect(body.rows).toBeNull();expect(body.query.trade_date).toBe('2026-10-21');
  }
});
test('all writes rejected, including settings request; invalid queries do not read',async()=>{
  const count=read.mock.calls.length;
  for(const method of ['POST','PUT','PATCH','DELETE'])expect((await fetch(base+'/settings/request',{method,headers:auth})).status).toBe(405);
  expect((await fetch(base+'/role/tr/today?trade_date=2026-02-30',{headers:auth})).status).toBe(400);
  expect((await fetch(base+'/day/2026-10-21/plans?limit=501',{headers:auth})).status).toBe(400);
  expect(read.mock.calls.length).toBe(count);
});
test('raw reader errors cannot leak paths or provider secrets',async()=>{
  read.mockRejectedValueOnce(new Error('/secret/private.db https://webhook.invalid'));
  const response=await fetch(base+'/settings',{headers:auth});expect(response.status).toBe(503);
  expect(await response.text()).toBe('{"error":"TEAM_UI_SOURCE_UNAVAILABLE"}');
});
test('single switch disables both real UI and API without accessing business dependencies',async()=>{
  expect(loadConfig({}).teamUiEnabled).toBe(false);
  const dependencies = {config:loadConfig({}),startedAt:Date.now()} as ApiDependencies;
  const isolated = createServer(createApp(dependencies));
  await new Promise<void>(done=>isolated.listen(0,'127.0.0.1',done));
  const url=`http://127.0.0.1:${(isolated.address() as {port:number}).port}`;
  try {
    expect((await fetch(url+'/team')).status).toBe(404);
    expect((await fetch(url+'/team/roles/tr')).status).toBe(404);
    expect((await fetch(url+'/api/ui/settings')).status).toBe(404);
  } finally {isolated.closeAllConnections();await new Promise<void>(done=>isolated.close(()=>done()));}
});
