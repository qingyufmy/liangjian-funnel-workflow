import { afterEach, expect, test, vi } from "vitest";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { MonitorHealth, sendInternalMonitorAlert } from "../../server/monitor-health.js";

const roots: string[] = [];
afterEach(async () => { await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true }))); });
async function setup() {
  const root = await mkdtemp(join(tmpdir(), "liangjian-h1-health-")); roots.push(root);
  const send = vi.fn(async (_file: string, _payload: unknown) => true);
  const log = vi.fn();
  return { root, send, log, health: new MonitorHealth(join(root, "health.json"), join(root, "shadow.env"), log, send) };
}
function record(minute: string, exitCode = 2) {
  return { runId: `monitor-20261016-${minute.replace(":", "")}`, startedAt: `2026-10-16T${minute}:03+08:00`,
    finishedAt: `2026-10-16T${minute}:10+08:00`, exitCode };
}

test("two distinct consecutive failures alert; repeated poll is not a new minute", async () => {
  const { health, send } = await setup();
  expect(health.observe(record("10:00"), { A4_TERMINAL_STATE_CONFLICT: 1 }).failureStreak).toBe(1);
  expect(health.observe(record("10:00"), { A4_TERMINAL_STATE_CONFLICT: 1 }).failureStreak).toBe(1);
  expect(send).not.toHaveBeenCalled();
  expect(health.observe(record("10:01"), { A4_TERMINAL_STATE_CONFLICT: 1 }).failureStreak).toBe(2);
  await health.flush();
  expect(send).toHaveBeenCalledTimes(1);
  expect(send.mock.calls[0]?.[1]).toMatchObject({ alertReason: "MONITOR_CONSECUTIVE_FAILURES" });
});

test("exit0 quarantine alerts immediately and survives Node restart without repeated send", async () => {
  const { health, root, send, log } = await setup();
  expect(health.observe(record("10:00", 0), { STATE_CONFLICT_QUARANTINED: 1 }).status).toBe("DEGRADED");
  await health.flush();
  expect(send).toHaveBeenCalledTimes(1);
  const restarted = new MonitorHealth(join(root, "health.json"), join(root, "shadow.env"), log, send);
  restarted.observe(record("10:01", 0), { STATE_CONFLICT_QUARANTINED: 1 });
  await restarted.flush();
  expect(send).toHaveBeenCalledTimes(1);
  expect(JSON.parse(await readFile(join(root, "health.json"), "utf8")).dailyReasonCounts.STATE_CONFLICT_QUARANTINED).toBe(2);
});

test("lunch, missing minute and a healthy minute break consecutive failure streak", async () => {
  const { health, send } = await setup();
  health.observe(record("11:30"), {});
  expect(health.observe(record("13:01"), {}).failureStreak).toBe(1);
  health.observe(record("13:02", 0), {});
  expect(health.observe(record("13:03"), {}).failureStreak).toBe(1);
  expect(health.observe(record("13:05"), {}).failureStreak).toBe(1);
  await health.flush(); expect(send).not.toHaveBeenCalled();
});

test("alert transport failure never throws or records webhook/error text", async () => {
  const { root, log } = await setup();
  const secret = "FAKE_LOCAL_FIXTURE_SECRET";
  const failing = vi.fn(async () => { throw Error(secret); });
  const health = new MonitorHealth(join(root, "failure.json"), join(root, "shadow.env"), log, failing);
  expect(() => health.observe(record("10:00", 0), { STATE_CONFLICT_QUARANTINED: 1 })).not.toThrow();
  await health.flush();
  expect(JSON.stringify(log.mock.calls)).not.toContain(secret);
  expect(await readFile(join(root, "failure.json"), "utf8")).not.toContain(secret);
});

test("internal sender never falls back to client hook and retries at most once", async () => {
  const request = vi.fn(async () => ({ ok: false, json: async () => ({ code: 1 }) }));
  const payload = { alertReason: "STATE_CONFLICT_QUARANTINED", runId: "fixture", reasonCounts: {} };
  const code = await sendInternalMonitorAlert("/missing-local-fixture.env", payload,
    { LARK_WEBHOOK_URL: "https://customer.invalid/never" }, request);
  expect(code).toBe(false); expect(request).not.toHaveBeenCalled();
  const result = await sendInternalMonitorAlert("/missing-local-fixture.env", payload,
    { SHADOW_RESEARCH_WEBHOOK: "https://open.feishu.cn/open-apis/bot/v2/hook/FAKE_NOT_A_REAL_CREDENTIAL" }, request);
  expect(result).toBe(false); expect(request).toHaveBeenCalledTimes(2);
});

test("new failure episode after recovery alerts again; stale polls do not roll health backwards", async () => {
  const { health, send } = await setup();
  health.observe(record("10:00"), {}); health.observe(record("10:01"), {}); await health.flush();
  health.observe(record("10:02", 0), {});
  health.observe(record("10:03"), {}); health.observe(record("10:04"), {}); await health.flush();
  expect(send).toHaveBeenCalledTimes(2);
  expect(health.observe(record("10:00"), {}).status).toBe("UNKNOWN");
  expect(health.observe(record("10:05", 0), {}).failureStreak).toBe(0);
});

test("first failure persisted before restart combines with next minute and alerts", async () => {
  const { health, root, send, log } = await setup();
  health.observe(record("10:00"), { A4_TERMINAL_STATE_CONFLICT:1 });
  const restarted = new MonitorHealth(join(root, "health.json"), join(root, "shadow.env"), log, send);
  expect(restarted.observe(record("10:01"), { A4_TERMINAL_STATE_CONFLICT:1 }).failureStreak).toBe(2);
  await restarted.flush(); expect(send).toHaveBeenCalledTimes(1);
});

test("authorized independent env parsed locally; credentials absent from outgoing content", async () => {
  const { root } = await setup();
  const file = join(root, "shadow.env");
  const fixture = "FAKE_NOT_A_REAL_CREDENTIAL";
  await writeFile(file, `OTHER_SECRET=DO_NOT_IMPORT\nSHADOW_RESEARCH_WEBHOOK='https://open.feishu.cn/open-apis/bot/v2/hook/${fixture}'\n`);
  const request=vi.fn(async (_url:string, _options:RequestInit) => ({ ok:true,json:async()=>({code:0}) }));
  expect(await sendInternalMonitorAlert(file,{alertReason:"STATE_CONFLICT_QUARANTINED",runId:"fixture",reasonCounts:{}},{},request)).toBe(true);
  expect(JSON.stringify(request.mock.calls[0]?.[1])).not.toContain(fixture);
  expect(process.env.OTHER_SECRET).not.toBe("DO_NOT_IMPORT");
});

test("business data failures are visible as degraded even with zero process exit", async () => {
  const { health, send }=await setup();
  expect(health.observe(record("10:00",0),{MINUTE_DATA_NOT_CURRENT:12})).toMatchObject({
    status:"DEGRADED",reasonCounts:{MINUTE_DATA_NOT_CURRENT:12},dailyReasonCounts:{MINUTE_DATA_NOT_CURRENT:12}});
  await health.flush();expect(send).not.toHaveBeenCalled();
});
