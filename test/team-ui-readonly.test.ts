import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { afterAll, beforeAll, expect, test } from 'vitest';
import { parseTeamUiQuery, TeamUiReadOnly } from '../server/team-ui-readonly.js';

const python = process.platform === 'win32' ? 'python' : 'python3';
const readerScript = resolve('scripts/team_ui_readonly.py');
let root: string;
let hashBefore: string;
const digest = (path: string) => createHash('sha256').update(readFileSync(path)).digest('hex');
beforeAll(() => {
  root = mkdtempSync(join(tmpdir(), 'team-ui-ro-test-'));
  mkdirSync(join(root, 'state'), { recursive: true });
  // Synthetic throwaway database only, not business code or a production copy.
  execFileSync(python, ['-B', '-c', `import sqlite3,sys,json
from pathlib import Path
p=Path(sys.argv[1]);c=sqlite3.connect(p/'state/workflow.sqlite3')
c.execute('CREATE TABLE execution_plans(plan_id,symbol,status,valid_from,expires_at,updated_at,payload_json)')
for i in range(3):
 c.execute('INSERT INTO execution_plans VALUES(?,?,?,?,?,?,?)',('plan:'+str(i),'000001.SZ','INVALIDATED' if i==0 else 'ACTIVE_TODAY','2026-10-21T09:26:00+08:00',None,'2026-10-21T10:00:00+08:00',json.dumps({'name':'测试股票','trigger_low':'10.0','trigger_high':'10.2','stop_level':'9.8','raw_response':'SENTINEL_SECRET','webhook':'https://secret.invalid','internal_path':'/secret/path','strategy_profile':'TREND_MA5','risk_unit':'PROBE'})))
c.commit();c.close()
`, root]);
  hashBefore = digest(join(root, 'state/workflow.sqlite3'));
});
afterAll(() => {
  if (!root) return;
  if (dirname(resolve(root)) !== resolve(tmpdir()) || !basename(root).startsWith('team-ui-ro-test-')) throw Error('UNSAFE_TEST_CLEANUP');
  rmSync(root, { recursive: true, force: true, maxRetries:10, retryDelay:50 });
});

test('calendar dates, parameter duplication and pagination rejected before I/O', () => {
  for (const value of ['2026-02-30', '2026-13-01', '../../secret'])
    expect(() => parseTeamUiQuery('/role/tr/today', new URLSearchParams({trade_date:value}), '2026-10-21')).toThrow();
  for (const value of ['limit=501', 'limit=1&limit=2', 'offset=-1', 'days=21', 'role=bogus'])
    expect(() => parseTeamUiQuery('/day/2026-10-21/plans', new URLSearchParams(value), '2026-10-21')).toThrow();
  expect(parseTeamUiQuery('/settings/request', new URLSearchParams(), '2026-10-21')).toBeNull();
  expect(parseTeamUiQuery('/role/bogus/today', new URLSearchParams(), '2026-10-21')).toBeNull();
});

function read(kind: string, extra = {}) {
  return JSON.parse(execFileSync(python, ['-B', readerScript, root, JSON.stringify({kind,trade_date:'2026-10-21',limit:2,offset:0,...extra})], { encoding:'utf8' }));
}
test('reads actual SQLite rows with bounded pagination and excludes raw private fields', () => {
  const page = read('plans');
  expect(page.status).toBe('AVAILABLE'); expect(page.rows).toHaveLength(2); expect(page.next_offset).toBe(2);
  expect(page.total).toBeNull(); expect(page.rows[0].status).toBe('INVALIDATED');
  expect(page.state_scope).toMatch(/NOT_HISTORICAL/);
  expect(page.rows[1].source.sha256).toMatch(/^[0-9a-f]{64}$/);
  expect(JSON.stringify(page)).not.toMatch(/SENTINEL_SECRET|webhook|https:|\/secret/);
  expect(read('plans', {offset:2}).rows).toHaveLength(1);
});
test('missing dataset remains null, never a made-up zero or activity', () => {
  const result = read('settings'); expect(result.status).toBe('MISSING_SOURCE'); expect(result.rows).toBeNull();
  expect(result.total).toBeNull();
});
test('no database mutation, new tables, files, WAL or sidecar', () => {
  read('plans');
  expect(digest(join(root, 'state/workflow.sqlite3'))).toBe(hashBefore);
  expect(readdirSync(join(root, 'state'))).toEqual(['workflow.sqlite3']);
});
test('transport has a fixed script and rejects missing source without creating databases', async () => {
  const query = parseTeamUiQuery('/day/2026-10-21/fills', new URLSearchParams(), '2026-10-21')!;
  // Missing script/source errors do not create any project files or run workflows.
  await expect(new TeamUiReadOnly(root, python).read(query)).rejects.toThrow();
  expect(readdirSync(join(root, 'state'))).toEqual(['workflow.sqlite3']);
});
