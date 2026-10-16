/** Internal business watchdog. Never uses the customer notification channel. */
import { mkdirSync, openSync, readSync, closeSync, writeFileSync, renameSync, unlinkSync } from "node:fs";
import { randomUUID } from "node:crypto";
import path from "node:path";

const CODES = new Set(["STATE_CONFLICT_QUARANTINED", "A4_LIFECYCLE_RECONCILIATION_REQUIRED",
  "A4_TERMINAL_STATE_CONFLICT", "A4_PLAN_IDENTITY_CONFLICT", "A4_SIGNAL_IDENTITY_CONFLICT",
  "ENTRY_CONTRACT_CLOCK_INVALID", "ENTRY_CONTRACT_CLOCK_CONFLICT", "STATE_CONFLICT",
  "PERSISTENCE_FAILED", "MONITOR_EXIT_NONZERO", "MONITOR_BUSINESS_STATUS_UNAVAILABLE"]);
type Counts = Record<string, number>;
type Logger = (message: string) => void;
interface MonitorResult { runId: string; startedAt: string; finishedAt: string | null; exitCode: number | null }
interface HealthState {
  version: 1; day: string; lastSlot: number; failureStreak: number;
  failureEpisodeStart: number; quarantineHighWater: number;
  dailyReasonCounts: Counts; notifiedKeys: string[]; lastAttempts: Record<string, number>;
}
export interface InternalAlert { alertReason: string; runId: string; reasonCounts: Counts }
export interface MonitorBusiness {
  status: "READY" | "DEGRADED" | "FAILED" | "UNKNOWN";
  reasonCounts: Counts; dailyReasonCounts: Counts; failureStreak: number;
}

function bounded(file: string, size = 64 * 1024): string | null {
  let fd: number | undefined;
  try {
    fd = openSync(file, "r"); const data = Buffer.alloc(size + 1);
    const n = readSync(fd, data, 0, data.length, 0);
    return n > size ? null : data.subarray(0, n).toString("utf8");
  } catch { return null; } finally { if (fd !== undefined) closeSync(fd); }
}
function counts(value: unknown): Counts {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(Object.entries(value).filter(([code, count]) =>
    /^[A-Z][A-Z0-9_]{1,79}$/.test(code) && Number.isSafeInteger(count)
      && Number(count) > 0 && Number(count) <= 100000).slice(0,64));
}
export function monitorReasonCounts(text: string): Counts {
  const result: Counts = {};
  for (const code of CODES) if (text.includes(code)) result[code] = 1;
  return result;
}

export async function sendInternalMonitorAlert(file: string, alert: InternalAlert,
  env: NodeJS.ProcessEnv = process.env,
  request: (url: string, options: RequestInit) => Promise<{ ok: boolean; json(): Promise<unknown> }> = fetch,
): Promise<boolean> {
  let webhook = env.SHADOW_RESEARCH_WEBHOOK?.trim();
  if (!webhook) {
    // Select exactly one authorized variable; never import the shadow env into
    // the process or fall back to LARK_WEBHOOK_URL / the customer settings.
    const line = bounded(file)?.split(/\r?\n/).find(value => /^\s*SHADOW_RESEARCH_WEBHOOK\s*=/.test(value));
    webhook = line?.slice(line.indexOf("=") + 1).trim().replace(/^(['"])(.*)\1$/, "$2");
  }
  if (!webhook || !/^https:\/\/open\.(feishu\.cn|larksuite\.com)\/open-apis\/bot\/v2\/hook\/[A-Za-z0-9_-]+$/.test(webhook)) return false;
  const safeCodes = counts(alert.reasonCounts);
  const content = `模拟盘｜A4系统告警\n${alert.alertReason}\nrun_id=${alert.runId}\n业务失败分布=${JSON.stringify(safeCodes)}\n不重启、不重跑、不修改历史或客户通知。`;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const response = await request(webhook, { method: "POST", redirect: "error",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ msg_type: "text", content: { text: content } }),
        signal: AbortSignal.timeout(2000) });
      const body = await response.json();
      if (response.ok && body && typeof body === "object" && "code" in body && body.code === 0) return true;
    } catch { /* Never log the exception: it can contain a credential URL. */ }
  }
  return false;
}

export class MonitorHealth {
  private state: HealthState;
  private readonly pending = new Map<string, Promise<void>>();
  constructor(private readonly stateFile: string, private readonly envFile: string,
    private readonly log: Logger,
    private readonly send: (file: string, alert: InternalAlert) => Promise<boolean> = sendInternalMonitorAlert,
  ) {
    this.state = this.empty("");
    try {
      const raw = JSON.parse(bounded(stateFile) ?? "null") as HealthState | null;
      if (raw?.version === 1 && /^\d{4}-\d{2}-\d{2}$/.test(raw.day)
          && Number.isSafeInteger(raw.lastSlot) && Number.isSafeInteger(raw.failureStreak)
          && raw.failureStreak >= 0 && raw.failureStreak <= 240 && Array.isArray(raw.notifiedKeys)) {
        this.state = { ...raw, dailyReasonCounts: counts(raw.dailyReasonCounts),
          failureEpisodeStart: Number.isSafeInteger(raw.failureEpisodeStart) ? raw.failureEpisodeStart : raw.lastSlot,
          quarantineHighWater: Number.isSafeInteger(raw.quarantineHighWater) ? raw.quarantineHighWater : 0,
          notifiedKeys: raw.notifiedKeys.filter(x => typeof x === "string").slice(-8),
          lastAttempts: raw.lastAttempts && typeof raw.lastAttempts === "object" ? raw.lastAttempts : {} };
      }
    } catch { this.log("MONITOR_HEALTH_PREVIOUS_STATE_UNREADABLE"); }
  }
  private empty(day: string): HealthState {
    return { version: 1, day, lastSlot: -1, failureStreak: 0, failureEpisodeStart: -1,
      quarantineHighWater: 0, dailyReasonCounts: {}, notifiedKeys: [], lastAttempts: {} };
  }
  private save(): void {
    const temporary = `${this.stateFile}.${randomUUID()}.tmp`;
    try {
      mkdirSync(path.dirname(this.stateFile), { recursive: true });
      writeFileSync(temporary, JSON.stringify(this.state), { mode: 0o600, flag: "wx" });
      renameSync(temporary, this.stateFile);
    } catch { this.log("MONITOR_HEALTH_STATE_WRITE_FAILED"); }
    finally { try { unlinkSync(temporary); } catch { /* already renamed or absent */ } }
  }
  public observe(record: MonitorResult, rawReasons: unknown): MonitorBusiness {
    const started = Date.parse(record.startedAt);
    if (!Number.isFinite(started)) return { status: "UNKNOWN", reasonCounts: {}, dailyReasonCounts: {}, failureStreak: 0 };
    const slot = Math.floor(started / 60000);
    const day = new Date(started + 8 * 3600000).toISOString().slice(0, 10);
    if (day < this.state.day || (day === this.state.day && slot < this.state.lastSlot))
      return { status:"UNKNOWN", reasonCounts:{}, dailyReasonCounts:{...this.state.dailyReasonCounts}, failureStreak:this.state.failureStreak };
    if (this.state.day !== day) this.state = this.empty(day);
    const reasonCounts = counts(rawReasons);
    const failed = record.exitCode !== 0;
    if (failed && !Object.keys(reasonCounts).length) reasonCounts.MONITOR_EXIT_NONZERO = 1;
    if (slot > this.state.lastSlot) {
      if (failed && (slot !== this.state.lastSlot + 1 || this.state.failureStreak === 0)) this.state.failureEpisodeStart = slot;
      this.state.failureStreak = failed ? (slot === this.state.lastSlot + 1 ? this.state.failureStreak + 1 : 1) : 0;
      this.state.lastSlot = slot;
      for (const [code, count] of Object.entries(reasonCounts)) this.state.dailyReasonCounts[code] = (this.state.dailyReasonCounts[code] ?? 0) + count;
    }
    const quarantined = Boolean(reasonCounts.STATE_CONFLICT_QUARANTINED || reasonCounts.A4_LIFECYCLE_RECONCILIATION_REQUIRED);
    if (quarantined) this.state.quarantineHighWater = Math.max(this.state.quarantineHighWater,
      reasonCounts.STATE_CONFLICT_QUARANTINED ?? reasonCounts.A4_LIFECYCLE_RECONCILIATION_REQUIRED ?? 1);
    const alertReason = quarantined ? "STATE_CONFLICT_QUARANTINED" : this.state.failureStreak >= 2 ? "MONITOR_CONSECUTIVE_FAILURES" : null;
    if (alertReason) {
      const episode = quarantined ? this.state.quarantineHighWater : this.state.failureEpisodeStart;
      const key = `${day}:${alertReason}:${episode}`;
      if (!this.state.notifiedKeys.includes(key) && !this.pending.has(key) && this.state.lastAttempts[key] !== slot) {
        this.state.lastAttempts[key] = slot;
        const task = Promise.resolve().then(() => this.send(this.envFile, { alertReason, runId: record.runId, reasonCounts }))
          .then(sent => {
            if (sent && this.state.day === day) this.state.notifiedKeys.push(key);
            this.log(sent ? "MONITOR_INTERNAL_ALERT_SENT" : "MONITOR_INTERNAL_ALERT_FAILED");
            this.save();
          }).catch(() => this.log("MONITOR_INTERNAL_ALERT_FAILED"))
          .finally(() => this.pending.delete(key));
        this.pending.set(key, task);
      }
    }
    this.save();
    return { status: failed ? "FAILED" : quarantined ? "DEGRADED"
      : reasonCounts.MONITOR_BUSINESS_STATUS_UNAVAILABLE && Object.keys(reasonCounts).length===1 ? "UNKNOWN"
      : Object.keys(reasonCounts).length ? "DEGRADED" : "READY",
      reasonCounts, dailyReasonCounts: { ...this.state.dailyReasonCounts }, failureStreak: this.state.failureStreak };
  }
  public async flush(): Promise<void> { await Promise.all(this.pending.values()); }
}
