import { mkdtemp, mkdir, readFile, readdir, rm, writeFile, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { expect, test, vi } from 'vitest';
import { buildTeamUiFailure, persistTeamUiError, redactTeamUiStderr, TeamUiEvidenceWriteError } from '../server/team-ui-error-evidence.js';
import { TeamUiReadOnly } from '../server/team-ui-readonly.js';

const query = { kind: 'plans', trade_date: '2026-10-22', limit: 50, offset: 0, days: 20 };
const input = { responseStatus: 503 as const, responseBody: { error: 'TEAM_UI_SOURCE_UNAVAILABLE' }, query,
  phase: 'RESPONSE_PREPARED_NOT_DELIVERY_PROOF' as const };
const sha = (value: string | Buffer): string => createHash('sha256').update(value).digest('hex');

test('bounded evidence preserves actual public body and captured stderr, never delivery proof', () => {
  const stderr = 'TEAM_UI_SOURCE_REASON:SOURCE_DB_MISSING\n';
  const body = buildTeamUiFailure({ ...input, error: { code: 1, stderr, stdout: 'CUSTOMER_PRIVATE', message: 'CREDENTIAL_PRIVATE' } }, new Date()) as any;
  expect(body).toMatchObject({ response_status: 503, response_body: '{"error":"TEAM_UI_SOURCE_UNAVAILABLE"}',
    delivery_proven: false, reason_code: 'SOURCE_DB_MISSING', phase: 'RESPONSE_PREPARED_NOT_DELIVERY_PROOF' });
  expect(body.child).toMatchObject({ stderr, captured_stderr_sha256: sha(stderr), captured_stderr_bytes: Buffer.byteLength(stderr), stdout_recorded: false });
  expect(JSON.stringify(body)).not.toContain('CUSTOMER_PRIVATE'); expect(JSON.stringify(body)).not.toContain('CREDENTIAL_PRIVATE');
});

test.each([
  [{ ...input, responseBody: { error: 'TEAM_UI_BUSY' } }, 'WORKER_POOL_BUSY'],
  [{ ...input, error: { killed: true, signal: 'SIGTERM', stderr: 'started' } }, 'CHILD_DEADLINE_TERMINATED'],
  [{ ...input, error: { stderr: 'TEAM_UI_SOURCE_REASON:SQLITE_BUSY_OR_LOCKED\n' } }, 'SQLITE_BUSY_OR_LOCKED'],
  [{ ...input, responseStatus: 500 as const, error: new SyntaxError('raw secret ignored') }, 'CHILD_INVALID_JSON'],
  [{ ...input, error: { stderr: 'database is locked (untrusted free prose)' } }, 'UNKNOWN_SOURCE_FAILURE'],
])('reason comes from known actual fields, not guessed raw error', (value, reason) => {
  expect(buildTeamUiFailure(value, new Date())).toMatchObject({ reason_code: reason });
});

test('redacts selected secrets, JSON credentials, authorization/cookie and authenticated URLs before truncation', () => {
  const raw = 'Authorization: Bearer abc123\n{"api_key":"key123","token":"tok123","password":"pass123"}\n' +
    'Cookie: session=priv123\nhttps://user:pw@host.invalid/api?token=leak\nknownSecretHere\nsk-abcdefg\n';
  const text = redactTeamUiStderr(raw, ['knownSecretHere']);
  for (const secret of ['abc123', 'key123', 'tok123', 'pass123', 'priv123', 'user:pw', 'leak', 'knownSecretHere', 'abcdefg']) expect(text).not.toContain(secret);
  const body = buildTeamUiFailure({ ...input, error: { stderr: raw.repeat(1000) } }, new Date(), ['knownSecretHere']) as any;
  expect(body.child.stderr).toContain('[TRUNCATED]'); expect(Buffer.byteLength(JSON.stringify(body))).toBeLessThan(16_384);
  expect(Buffer.byteLength(body.child.stderr)).toBeLessThanOrEqual(4_096);
  expect(body.child.captured_stderr_bytes).toBe(Buffer.byteLength(raw.repeat(1000)));
});

test('success status or arbitrary query/path cannot create an error receipt', () => {
  expect(() => buildTeamUiFailure({ ...input, responseStatus: 200 as any }, new Date())).toThrow(TeamUiEvidenceWriteError);
  expect(() => buildTeamUiFailure({ ...input, query: { kind: '../../.env', trade_date: '2026-10-22' } }, new Date())).toThrow(TeamUiEvidenceWriteError);
});

test('exclusive private files are unique, bounded and secret-free on real disk', async () => {
  const root = await mkdtemp(join(tmpdir(), 'team-ui-errors-0615-'));
  const previous = process.env.LIANGJIAN_TEAM_UI_TOKEN;
  process.env.LIANGJIAN_TEAM_UI_TOKEN = 'known-ui-private-credential';
  try {
    const one = await persistTeamUiError(root, { ...input, error: { stderr: 'known-ui-private-credential' } });
    const two = await persistTeamUiError(root, input);
    expect(one.path).not.toBe(two.path);
    const raw = await readFile(one.path); expect(sha(raw)).toBe(one.sha256); expect(raw.length).toBeLessThanOrEqual(16_384);
    expect(raw.toString()).not.toContain('known-ui-private-credential');
    if (process.platform !== 'win32') expect((await stat(one.path)).mode & 0o777).toBe(0o600);
  } finally {
    if (previous === undefined) delete process.env.LIANGJIAN_TEAM_UI_TOKEN; else process.env.LIANGJIAN_TEAM_UI_TOKEN = previous;
    await rm(root, { recursive: true, force: true });
  }
});

test('write failure is explicit, never a persisted success; recordFailure logs only fixed failure code', async () => {
  const root = await mkdtemp(join(tmpdir(), 'team-ui-errors-fail-0615-'));
  const warn = vi.spyOn(console, 'error').mockImplementation(() => undefined);
  try {
    await writeFile(join(root, 'outputs'), 'BLOCKING_FILE');
    await expect(persistTeamUiError(root, input)).rejects.toThrow('TEAM_UI_ERROR_EVIDENCE_WRITE_FAILED');
    const reader = new TeamUiReadOnly(root, 'unused-no-child-called');
    await expect(reader.recordFailure(query, { stderr: 'secret-must-not-be-console' }, input.responseBody)).rejects.toThrow(TeamUiEvidenceWriteError);
    expect(warn.mock.calls).toEqual([['TEAM_UI_ERROR_EVIDENCE_WRITE_FAILED']]);
  } finally { warn.mockRestore(); await rm(root, { recursive: true, force: true }); }
});

test('actual read success writes no evidence; malformed success captures stderr only after API failure', async () => {
  const root = await mkdtemp(join(tmpdir(), 'team-ui-worker-0615-'));
  try {
    await mkdir(join(root, 'scripts'));
    const worker = join(root, 'scripts/team_ui_readonly.py');
    await writeFile(worker, 'print("{}")\n');
    const reader = new TeamUiReadOnly(root, process.platform === 'win32' ? 'python' : 'python3');
    await expect(reader.read(query)).resolves.toEqual({}); expect(await readdir(root)).toEqual(['scripts']);
    await writeFile(worker, 'import sys\nprint("TEAM_UI_SOURCE_REASON:UNKNOWN_SOURCE_FAILURE",file=sys.stderr)\nprint("invalid-json")\n');
    const broken = new TeamUiReadOnly(root, process.platform === 'win32' ? 'python' : 'python3');
    let error: unknown; try { await broken.read(query); } catch (value) { error = value; }
    expect(error).toBeInstanceOf(SyntaxError); expect((error as any).stderr).toContain('TEAM_UI_SOURCE_REASON:UNKNOWN_SOURCE_FAILURE');
    const ref = await broken.recordFailure(query, error, input.responseBody) as { path: string };
    expect(JSON.parse(await readFile(ref.path, 'utf8')).child.stderr).toContain('TEAM_UI_SOURCE_REASON:UNKNOWN_SOURCE_FAILURE');
  } finally { await rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 50 }); }
});
