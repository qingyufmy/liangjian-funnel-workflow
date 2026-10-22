import {test,expect} from 'vitest';
import {parseMockRoute,projectMockResponse} from '../scripts/team_mock_api.mjs';
const q=(text='')=>new URLSearchParams(text);
test('all brief GET signatures exist; no settings write route or wrong historical day',()=>{
  for(const path of ['/day/2026-10-20/tasks','/day/2026-10-20/feed','/day/2026-10-20/plans','/day/2026-10-20/fills','/role/rk/today','/role/rk/stats','/review/scoreboard','/review/lessons','/settings'])expect(parseMockRoute('/api/ui'+path,q()).error).toBeUndefined();
  expect(parseMockRoute('/api/ui/settings/request',q()).error).toBe(404);
  expect(parseMockRoute('/api/ui/day/2026-10-21/plans',q()).error).toBe(404);
});
test('pagination and query constraints are bounded before any evidence load',()=>{
  for(const query of ['limit=501','limit=0','limit=abc','days=21','days=abc','offset=-1','role=bogus'])expect(parseMockRoute('/api/ui/day/2026-10-20/plans',q(query)).error).toBe(400);
  expect(parseMockRoute('/api/ui/role/fake/today',q()).error).toBe(404);
});
test('paginated rows preserve source objects and distinguish unknown from known empty fills',()=>{
  const fixture={trade_date:'2026-10-20',kind:'PARTIAL_FROZEN_EVIDENCE',sources:{},coverage:{paper_counts:{paper_fills:0}},events:[{type:'plans',rows:Array.from({length:33},(_,i)=>({id:i}))}]};
  const route=parseMockRoute('/api/ui/day/2026-10-20/plans',q('limit=10&offset=20'));
  const result=projectMockResponse(route,fixture);expect(result.rows).toHaveLength(10);expect(result.total).toBe(33);expect(result.next_offset).toBe(30);
  expect(projectMockResponse({...route,kind:'fills'},fixture).rows).toEqual([]);
  expect(projectMockResponse({...route,kind:'feed'},fixture).rows).toBeNull();expect(fixture.events[0].rows).toHaveLength(33);
});
test('absent role and stats never turn into fabricated activity or PnL',()=>{
  const fixture={trade_date:'2026-10-20',kind:'PARTIAL_FROZEN_EVIDENCE',sources:{},coverage:{paper_counts:{paper_fills:0}},events:[]};
  const route=parseMockRoute('/api/ui/role/a1/today',q());
  const result=projectMockResponse(route,fixture);expect(result.status).toBe('MISSING_SOURCE');expect(result.rows).toBeNull();expect(result.total).toBeNull();
});
