import { createHash, randomUUID } from 'node:crypto';
import { lstat, mkdir, open, realpath } from 'node:fs/promises';
import { join, resolve, sep } from 'node:path';

const MAX_STDERR_BYTES = 4_096;
const MAX_RECEIPT_BYTES = 16_384;
const SAFE_ERROR = /^[A-Z][A-Z0-9_]{0,95}$/;
const SOURCE_CODES = new Set(['SOURCE_DB_MISSING', 'SQLITE_BUSY_OR_LOCKED', 'UNKNOWN_SOURCE_FAILURE']);
export interface TeamUiFailureQuery { readonly kind: string; readonly trade_date: string }
export interface TeamUiFailureInput {
  readonly responseStatus: 500 | 503;
  readonly responseBody: { readonly error: string };
  readonly error?: unknown;
  readonly query: TeamUiFailureQuery;
  readonly phase: 'RESPONSE_PREPARED_NOT_DELIVERY_PROOF';
}
export class TeamUiEvidenceWriteError extends Error {
  constructor() { super('TEAM_UI_ERROR_EVIDENCE_WRITE_FAILED'); }
}

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' ? value as Record<string, unknown> : {};
}
function hash(value: string | Buffer): string { return createHash('sha256').update(value).digest('hex'); }

/** Selected known process credentials only; never inspect .env/all env/request. */
function knownSecrets(): string[] {
  return ['LIANGJIAN_TEAM_UI_TOKEN', 'LIANGJIAN_DASHBOARD_TOKEN', 'LIANGJIAN_MODEL_API_KEY']
    .map(key => process.env[key]).filter((v): v is string => typeof v === 'string' && v.length > 0);
}

export function redactTeamUiStderr(value: string, secrets: readonly string[] = []): string {
  let text = value;
  for (const secret of [...secrets].sort((a, b) => b.length - a.length)) if (secret) text = text.split(secret).join('[REDACTED]');
  text = text.replace(/https?:\/\/[^\s<>"']+/gi, '[REDACTED_URL]');
  text = text.replace(/\bBearer\s+[A-Za-z0-9._~+/=-]+/gi, 'Bearer [REDACTED]');
  text = text.replace(/\bsk-[A-Za-z0-9][A-Za-z0-9_-]*/g, 'sk-[REDACTED]');
  text = text.replace(/(["']?(?:authorization|api[_ -]?key|access[_ -]?key|secret|password|passwd|credential|token|cookie|set-cookie)["']?\s*[:=]\s*)(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s,;}]+)/gi, '$1[REDACTED]');
  // A child must not turn model/customer request objects into an error log.
  text = text.replace(/(["'](?:messages|prompt|reasoning_content|customer_payload)["']\s*:\s*)(?:"(?:\\.|[^"\\])*"|\[[^\r\n]*\]|\{[^\r\n]*\})/gi, '$1"[REDACTED_PRIVATE_CONTENT]"');
  const bytes = Buffer.from(text);
  if (bytes.length <= MAX_STDERR_BYTES) return text;
  const suffix = '[TRUNCATED]';
  const limit = MAX_STDERR_BYTES - Buffer.byteLength(suffix);
  let prefix = bytes.subarray(0, limit).toString('utf8');
  while (Buffer.byteLength(prefix) > limit) prefix = prefix.slice(0, -1);
  return prefix + suffix;
}

function safeCode(value: unknown): string | number | null {
  if (typeof value === 'number' && Number.isSafeInteger(value)) return value;
  return typeof value === 'string' && /^[A-Z0-9_]{1,80}$/.test(value) ? value : null;
}

export function buildTeamUiFailure(input: TeamUiFailureInput, at: Date, secrets: readonly string[] = []): object {
  if (![500, 503].includes(input.responseStatus) || input.phase !== 'RESPONSE_PREPARED_NOT_DELIVERY_PROOF' ||
      !SAFE_ERROR.test(input.responseBody.error) || !/^[a-z0-9/-]{1,80}$/.test(input.query.kind) ||
      !/^\d{4}-\d{2}-\d{2}$/.test(input.query.trade_date) || !Number.isFinite(at.getTime())) throw new TeamUiEvidenceWriteError();
  const error = record(input.error);
  // Only explicitly captured stderr/code/kill fields. Never stack/message/stdout,
  // arbitrary Error properties, request headers, raw query or customer body.
  const stderr = typeof error.stderr === 'string' ? error.stderr : Buffer.isBuffer(error.stderr) ? error.stderr.toString('utf8') : '';
  const marker = /^TEAM_UI_SOURCE_REASON:(SOURCE_DB_MISSING|SQLITE_BUSY_OR_LOCKED|UNKNOWN_SOURCE_FAILURE)\s*$/m.exec(stderr)?.[1];
  const sourceCode = marker && SOURCE_CODES.has(marker) ? marker : null;
  const reason = input.responseBody.error === 'TEAM_UI_BUSY' ? 'WORKER_POOL_BUSY' :
    error.killed === true ? 'CHILD_DEADLINE_TERMINATED' :
    error.code === 'ERR_CHILD_PROCESS_STDIO_MAXBUFFER' ? 'CHILD_STDIO_LIMIT' :
    sourceCode ?? (input.error instanceof SyntaxError ? 'CHILD_INVALID_JSON' : 'UNKNOWN_SOURCE_FAILURE');
  const publicBody = JSON.stringify({ error: input.responseBody.error });
  return {
    schema: 'team-ui-error-evidence/0615', captured_at: at.toISOString(),
    phase: input.phase, delivery_proven: false, response_status: input.responseStatus,
    response_body: publicBody, response_body_sha256: hash(publicBody),
    query: { kind: input.query.kind, trade_date: input.query.trade_date }, reason_code: reason,
    child: { code: safeCode(error.code), killed: error.killed === true, signal: safeCode(error.signal),
      captured_stderr_bytes: Buffer.byteLength(stderr), captured_stderr_sha256: hash(stderr),
      stderr: redactTeamUiStderr(stderr, secrets), stderr_limit_bytes: MAX_STDERR_BYTES,
      stdout_recorded: false, exception_message_or_stack_recorded: false },
  };
}

async function privateDirectory(parent: string, child: string): Promise<string> {
  const path = join(parent, child);
  try { await mkdir(path, { mode: 0o700 }); }
  catch (error) { if (record(error).code !== 'EEXIST') throw error; }
  const entry = await lstat(path);
  if (!entry.isDirectory() || entry.isSymbolicLink() || await realpath(path) !== resolve(path)) throw new TeamUiEvidenceWriteError();
  return path;
}

/** One actual API failure -> one exclusive private receipt. Success never calls.
 * Output is bounded per file, with no credentials, business DB/queue writes or
 * claimed network delivery. A write failure rejects explicitly; do not fake PASS.
 */
export async function persistTeamUiError(rootDir: string, input: TeamUiFailureInput): Promise<{path: string; sha256: string}> {
  try {
    const root = await realpath(rootDir);
    const at = new Date();
    const body = buildTeamUiFailure(input, at, knownSecrets());
    const raw = Buffer.from(JSON.stringify(body));
    if (raw.length > MAX_RECEIPT_BYTES) throw new TeamUiEvidenceWriteError();
    const outputs = await privateDirectory(root, 'outputs');
    const errors = await privateDirectory(outputs, 'ui-errors');
    const directory = await privateDirectory(errors, at.toISOString().slice(0, 10));
    if (!directory.startsWith(root + sep)) throw new TeamUiEvidenceWriteError();
    const path = join(directory, at.toISOString().replace(/[:.]/g, '-') + '-' + randomUUID() + '.json');
    const file = await open(path, 'wx', 0o600);
    try { await file.writeFile(raw); await file.sync(); }
    finally { await file.close(); }
    return { path, sha256: hash(raw) };
  } catch { throw new TeamUiEvidenceWriteError(); }
}
