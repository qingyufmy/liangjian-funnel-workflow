import { ProjectFiles } from "./files.js";
import { LogStore } from "./logger.js";
import { asArray, asJsonRecord, asString, sanitizeJson } from "./redaction.js";
import { JobRunner } from "./runner.js";
import { WorkflowScheduler } from "./scheduler.js";
import type { AppConfig } from "./config.js";
import type { JsonRecord, JsonValue, StatusSnapshot } from "./types.js";

function record(value: unknown): JsonRecord | null {
  return asJsonRecord(value);
}

function stringField(value: unknown, key: string): string | null {
  const source = record(value);
  return source ? asString(source[key]) : null;
}

function arrayField(value: unknown, key: string): readonly unknown[] {
  const source = record(value);
  return source ? asArray(source[key]) ?? [] : [];
}

function parseJsonArray(value: unknown): JsonValue | null {
  if (typeof value !== "string") return null;
  try {
    return sanitizeJson(JSON.parse(value));
  } catch {
    return null;
  }
}

function planCount(status: JsonRecord | null, name: string): number | null {
  const plans = status ? record(status.plan_counts) : null;
  const value = plans?.[name];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function normalizeAccounts(status: JsonRecord): JsonValue | null {
  const accounts = arrayField(status, "accounts");
  if (!accounts.length) return [];
  const positions = record(status.positions);
  return sanitizeJson(accounts.map((item) => {
    const account = record(item);
    if (!account) return null;
    const accountId = asString(account.account_id);
    const accountPositions = accountId && positions ? positions[accountId] : null;
    return {
      accountId,
      account_id: accountId,
      model: asString(account.model),
      status: asString(account.status),
      cash: account.cash ?? null,
      equity: account.equity ?? null,
      initialCash: account.initial_cash ?? null,
      positions: Array.isArray(accountPositions) ? accountPositions.length : null,
    };
  }));
}

function normalizeMonitorEvent(value: unknown): JsonValue {
  const event = record(value);
  if (!event) return sanitizeJson(value);
  return sanitizeJson({
    ...event,
    time: event.time ?? event.minute_end ?? null,
    minuteEnd: event.minuteEnd ?? event.minute_end ?? null,
    laneId: event.laneId ?? event.lane_id ?? null,
    reasonCode: event.reasonCode ?? event.reason_code ?? null,
  });
}

function normalizeStage(value: unknown): JsonValue {
  const stage = record(value);
  if (!stage) return sanitizeJson(value);
  const symbols = stage.symbols;
  const latency = stage.latency_ms;
  const reasonCodes = stage.reason_codes ?? stage.reasonCodes ?? null;
  return sanitizeJson({
    ...stage,
    stage: asString(stage.stage),
    status: asString(stage.status),
    symbolCount: Array.isArray(symbols) ? symbols.length : null,
    latencyMs: typeof latency === "number" && Number.isFinite(latency) ? latency : null,
    reasonCodes,
  });
}

function latestRows(status: StatusSnapshot): readonly JsonRecord[] {
  if (!status.data) return [];
  return arrayField(status.data, "latest_workflow_runs")
    .map((item) => record(item))
    .filter((item): item is JsonRecord => item !== null);
}

function runIdFromStatus(status: StatusSnapshot): string | null {
  const acceptance = record(status.data?.latest_workflow_acceptance);
  const accepted = acceptance ? asString(acceptance.run_id) : null;
  if (accepted) return accepted;
  const first = latestRows(status)[0];
  return first ? asString(first.run_id) : null;
}

function laneOverview(laneId: string, rows: readonly JsonRecord[], payload: JsonRecord | null): JsonValue {
  const row = rows.find((item) => item.lane_id === laneId) ?? null;
  const payloadLane = payload && stringField(payload, "lane") === laneId ? payload : null;
  const stages = payloadLane
    ? arrayField(payloadLane, "stages").map((stage) => normalizeStage(stage))
    : null;
  const model = row ? asString(row.model) : payloadLane ? asString(payloadLane.model) : null;
  const stageStatuses = row ? parseJsonArray(row.reason_codes_json) : null;
  return sanitizeJson({
    laneId,
    model,
    status: row ? asString(row.status) : payloadLane ? asString(payloadLane.status) : null,
    runId: row ? asString(row.run_id) : null,
    slot: row ? asString(row.slot) : null,
    stages,
    reasonCodes: stageStatuses,
    source: payloadLane ? "outputs/research" : row ? "state/status" : null,
  });
}

export class DashboardData {
  public constructor(
    private readonly config: AppConfig,
    private readonly files: ProjectFiles,
    private readonly runner: JobRunner,
    private readonly scheduler: WorkflowScheduler,
    private readonly logger: LogStore,
  ) {}

  public async overview(): Promise<JsonValue> {
    const status = await this.files.status();
    const runs = await this.files.listRuns(20);
    const runId = runIdFromStatus(status) ?? runs[0]?.runId ?? null;
    const lanesPayload = await this.files.latestResearchLanes(runId);
    const laneRecords = (lanesPayload ?? [])
      .map((item) => record(item))
      .filter((item): item is JsonRecord => item !== null);
    const rows = latestRows(status);
    const monitor = await this.files.monitor();
    const dataSources = (await this.files.dataSources()).map((source) => ({
      ...source,
      id: source.provider,
      label: source.provider,
      checkedAt: source.generatedAt,
      detail: source.path,
    }));
    const statusData = status.data;
    const acceptance = record(statusData?.latest_workflow_acceptance);
    const latestRun = runs.find((item) => item.runId === runId) ?? null;
    const lanes = ["lane_1", "lane_2", "lane_3"].map((laneId) => laneOverview(
      laneId,
      rows,
      laneRecords.find((item) => item.lane === laneId) ?? null,
    ));
    const latestWorkflow = runId
      ? sanitizeJson({
        runId,
        slot: rows[0] ? asString(rows[0].slot) : latestRun?.slot ?? null,
        status: acceptance ? asString(acceptance.status) : latestRun?.status ?? null,
        tradeDate: rows[0] ? asString(rows[0].trade_date) : null,
        snapshotHash: rows[0] ? asString(rows[0].snapshot_hash) : null,
        createdAt: rows[0] ? asString(rows[0].created_at) : null,
        updatedAt: rows[0] ? asString(rows[0].updated_at) : null,
        researchMarkdown: latestRun?.researchMarkdown ?? null,
        acceptance: acceptance ? sanitizeJson(acceptance) : null,
        lanes,
      })
      : null;
    const monitorRecord = record(monitor.latest);
    const monitorLanes = monitorRecord ? arrayField(monitorRecord, "lanes") : [];
    const monitorStatus = monitor.latest === null
      ? null
      : monitorLanes.some((lane) => record(lane)?.blocked === true) ? "blocked" : "ok";
    const scheduleSnapshot = this.scheduler.snapshot();
    const accounts = statusData ? normalizeAccounts(statusData) : null;
    const planCounts = statusData ? statusData.plan_counts ?? null : null;
    const monitorEvents = monitorRecord
      ? monitorLanes.flatMap((lane) => arrayField(lane, "events").map((event) => normalizeMonitorEvent(event)))
      : [];
    const effectiveEvents = monitor.events.map((event) => normalizeMonitorEvent(event));
    const serviceHealthy = status.availability === "ok"
      && statusData?.configuration_ready !== false
      && statusData?.state_healthy !== false;
    return sanitizeJson({
      generatedAt: new Date().toISOString(),
      service: {
        status: serviceHealthy ? "healthy" : "warning",
        uptimeSeconds: Math.floor(process.uptime()),
        timezone: "Asia/Shanghai",
        host: `${this.config.host}:${this.config.port}`,
        stateHealthy: statusData ? statusData.state_healthy ?? null : null,
        configurationReady: statusData ? statusData.configuration_ready ?? null : null,
        deploymentReady: statusData ? statusData.deployment_ready ?? null : null,
        blockers: statusData ? statusData.deployment_blockers ?? null : null,
        schedulerEnabled: this.config.schedulerEnabled,
        pythonStatus: status.availability,
        pythonStatusReason: status.reason,
      },
      schedule: scheduleSnapshot.jobs.map((item) => ({
        id: item.job,
        job: item.job,
        label: item.label,
        time: item.schedule,
        cron: item.schedule,
        status: item.enabled ? "ACTIVE" : "DISABLED",
        nextRunAt: null,
      })),
      scheduleMeta: scheduleSnapshot,
      activeJob: this.runner.activeJob(),
      recentJobRuns: this.runner.recentRuns(10),
      workflowStatus: status.data
        ? {
          configurationReady: statusData ? statusData.configuration_ready ?? null : null,
          deploymentReady: statusData ? statusData.deployment_ready ?? null : null,
          blockers: statusData ? statusData.deployment_blockers ?? null : null,
          latestAcceptance: acceptance ? sanitizeJson(acceptance) : null,
        }
        : null,
      latestWorkflow,
      lanes,
      monitor: {
        status: monitorStatus,
        checkedAt: monitorRecord ? asString(monitorRecord.time) : null,
        effectiveEventCount: monitor.events.length,
        activePlanCount: planCount(statusData, "ACTIVE_TODAY"),
        events: monitorEvents,
        latest: monitor.latest,
        effectiveSignals: monitor.effectiveSignals,
        laneCount: monitorLanes.length,
      },
      accounts,
      positions: statusData ? statusData.positions ?? null : null,
      plans: planCounts,
      planCounts,
      fills: statusData ? statusData.fills ?? null : null,
      dataSources,
      recentEffectiveEvents: effectiveEvents,
      recentLogs: await this.logger.list(20),
    });
  }

  public async runs(limit: number): Promise<JsonValue> {
    return sanitizeJson({ runs: await this.files.listRuns(limit), limit });
  }

  public async run(runId: string): Promise<JsonValue | null> {
    const detail = await this.files.getRun(runId);
    return detail ? sanitizeJson(detail) : null;
  }

  public async logs(limit: number, level?: "debug" | "info" | "warn" | "error", job?: string): Promise<JsonValue> {
    return sanitizeJson({ logs: await this.logger.list(limit, level, job), limit, level: level ?? null, job: job ?? null });
  }
}
