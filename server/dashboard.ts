import { ProjectFiles, normalizeLaneOutcome, normalizeRunOutcome, normalizeStageOutcome } from "./files.js";
import { LogStore } from "./logger.js";
import { asArray, asJsonRecord, asString, sanitizeJson } from "./redaction.js";
import { JobRunner } from "./runner.js";
import { WorkflowScheduler } from "./scheduler.js";
import type { AppConfig } from "./config.js";
import type {
  JobRunRecord,
  JsonRecord,
  JsonValue,
  LogEvent,
  MonitorDispatchStatus,
  MonitorDispatchSummary,
  StatusSnapshot,
} from "./types.js";

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

function safeCount(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? Math.floor(value)
    : null;
}

export function normalizeStagePoolCounts(stage: JsonRecord): JsonValue | null {
  const stageName = asString(stage.stage)?.toUpperCase();
  const output = record(stage.output);
  if (!stageName || !output) return null;

  const poolNames = stageName === "A1"
    ? { approved: "active_research_pool", watch: "monitor_pool", rejected: "rejected_candidates" }
    : stageName === "A2"
      ? { approved: "focus_pool", watch: "watch_only_pool", rejected: "rejected_candidates" }
      : stageName === "A3"
        ? { approved: "core_watch_pool", watch: "secondary_watch_pool", rejected: "rejected_candidates" }
        : null;
  if (!poolNames) return null;

  const approved = arrayField(output, poolNames.approved).length;
  const watch = arrayField(output, poolNames.watch).length;
  const rejected = arrayField(output, poolNames.rejected).length;
  const summary = record(output.analysis_summary);
  const effectiveResearch = stageName === "A2"
    ? safeCount(summary?.effective_research_pool_count) ?? approved + watch
    : approved;
  const a3Candidates = stageName === "A2" ? safeCount(summary?.a3_candidate_count) : null;
  const rotationDirections = stageName === "A2"
    ? safeCount(summary?.rotation_direction_count) ?? arrayField(output, "active_themes").length
    : null;

  return sanitizeJson({
    approved,
    watch,
    rejected,
    effectiveResearch,
    a3Candidates,
    rotationDirections,
  });
}

function parseJsonArray(value: unknown): JsonValue | null {
  if (typeof value !== "string") return null;
  try {
    return sanitizeJson(JSON.parse(value));
  } catch {
    return null;
  }
}

function parseJsonObject(value: unknown): JsonRecord | null {
  if (typeof value !== "string") return null;
  try {
    return record(JSON.parse(value));
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
    planId: event.planId ?? event.plan_id ?? null,
    llmVeto: event.llmVeto ?? event.llm_veto ?? false,
    reasonCode: event.reasonCode ?? event.reason_code ?? null,
    diagnosticCode: event.diagnosticCode ?? event.diagnostic_code ?? null,
    strategyProfile: event.strategyProfile ?? event.strategy_profile ?? null,
    eligibility: event.eligibility ?? null,
    reasonCodes: event.reasonCodes ?? event.reason_codes ?? [],
    metConditions: event.metConditions ?? event.met_conditions ?? [],
    unmetConditions: event.unmetConditions ?? event.unmet_conditions ?? [],
    vetoConditions: event.vetoConditions ?? event.veto_conditions ?? [],
    closed5mEnd: event.closed5mEnd ?? event.closed_5m_end ?? null,
    closed15mEnd: event.closed15mEnd ?? event.closed_15m_end ?? null,
    plan: normalizeMonitorPlan(event.plan),
    simulation: normalizeSimulation(event.simulation),
  });
}

function normalizeMonitorPlan(value: unknown): JsonValue | null {
  const plan = record(value);
  if (!plan) return null;
  return sanitizeJson({
    planId: plan.planId ?? plan.plan_id ?? null,
    laneId: plan.laneId ?? plan.lane_id ?? null,
    symbol: plan.symbol ?? null,
    name: plan.name ?? null,
    status: plan.status ?? null,
    validFrom: plan.validFrom ?? plan.valid_from ?? null,
    expiresAt: plan.expiresAt ?? plan.expires_at ?? null,
    strategyProfile: plan.strategyProfile ?? plan.strategy_profile ?? null,
    planPriority: plan.planPriority ?? plan.plan_priority ?? null,
    priorityReasons: plan.priorityReasons ?? plan.priority_reasons ?? [],
    eligibility: plan.eligibility ?? null,
    setupType: plan.setupType ?? plan.setup_type ?? null,
    triggerLow: plan.triggerLow ?? plan.trigger_low ?? null,
    triggerHigh: plan.triggerHigh ?? plan.trigger_high ?? null,
    stopLevel: plan.stopLevel ?? plan.stop_level ?? null,
    riskUnit: plan.riskUnit ?? plan.risk_unit ?? null,
    noChasePrice: plan.noChasePrice ?? plan.no_chase_price ?? plan.max_chase_price ?? null,
    referencePrice: plan.referencePrice ?? plan.reference_price ?? null,
    referencePriceAsOf: plan.referencePriceAsOf ?? plan.reference_price_as_of ?? null,
    pressureReducePrice: plan.pressureReducePrice ?? plan.pressure_reduce_price ?? null,
    pressureBasis: plan.pressureBasis ?? plan.pressure_basis ?? null,
    requiredConditions: plan.requiredConditions ?? plan.required_conditions ?? [],
    metConditions: plan.metConditions ?? plan.met_conditions ?? [],
    unmetConditions: plan.unmetConditions ?? plan.unmet_conditions ?? [],
    vetoConditions: plan.vetoConditions ?? plan.veto_conditions ?? [],
    sourceRunId: plan.sourceRunId ?? plan.source_run_id ?? null,
    selectionReasons: plan.selectionReasons ?? plan.selection_reasons ?? [],
  });
}

function normalizeSimulation(value: unknown): JsonValue | null {
  const simulation = record(value);
  if (!simulation) return null;
  return sanitizeJson({
    status: simulation.status ?? null,
    action: simulation.action ?? null,
    qty: simulation.qty ?? null,
    price: simulation.price ?? null,
    fee: simulation.fee ?? null,
    barEnd: simulation.barEnd ?? simulation.bar_end ?? null,
  });
}

/**
 * Normalize the durable Python lifecycle row once at the Node boundary.
 * Older deployments may expose the SQLite column names (``signal_time``,
 * ``entry_time`` and so on), while the lifecycle query contract exposes the
 * explicit ``*_signal_*``/``*_fill_*`` names.  Accept both here so a rolling
 * update never makes the read-only workbench crash or silently lose a field.
 */
export function normalizeA4SignalLifecycle(value: unknown): JsonValue | null {
  const lifecycle = record(value);
  if (!lifecycle) return null;
  const field = (camel: string, snake: string, fallback: unknown = null): unknown =>
    lifecycle[camel] ?? lifecycle[snake] ?? fallback;
  return sanitizeJson({
    lifecycleId: field("lifecycleId", "lifecycle_id"),
    planId: field("planId", "plan_id"),
    sourceRunId: field("sourceRunId", "source_run_id"),
    laneId: field("laneId", "lane_id"),
    accountId: field("accountId", "account_id"),
    tradeDate: field("tradeDate", "trade_date"),
    symbol: field("symbol", "symbol"),
    name: field("name", "name"),
    stockBehaviorType: field("stockBehaviorType", "stock_behavior_type"),
    strategyProfile: field("strategyProfile", "strategy_profile"),
    status: field("status", "status"),
    entrySignalAt: field("entrySignalAt", "entry_signal_at", field("signalTime", "signal_time")),
    entrySignalPrice: field("entrySignalPrice", "entry_signal_price", field("signalPrice", "signal_price")),
    entryFillAt: field("entryFillAt", "entry_fill_at", field("entryTime", "entry_time")),
    entryFillPrice: field("entryFillPrice", "entry_fill_price", field("entryPrice", "entry_price")),
    entryQty: field("entryQty", "entry_qty"),
    entryFee: field("entryFee", "entry_fee"),
    currentQty: field("currentQty", "current_qty", field("remainingQty", "remaining_qty")),
    maxPrice: field("maxPrice", "max_price"),
    minPrice: field("minPrice", "min_price"),
    mfePct: field("mfePct", "mfe_pct", field("mfe", "mfe")),
    maePct: field("maePct", "mae_pct", field("mae", "mae")),
    exitSignalAt: field("exitSignalAt", "exit_signal_at", field("exitSignalTime", "exit_signal_time")),
    exitFillAt: field("exitFillAt", "exit_fill_at", field("exitTime", "exit_time")),
    exitFillPrice: field("exitFillPrice", "exit_fill_price", field("exitPrice", "exit_price")),
    exitQty: field("exitQty", "exit_qty"),
    exitFee: field("exitFee", "exit_fee"),
    exitReasonCode: field("exitReasonCode", "exit_reason_code", field("exitReason", "exit_reason")),
    grossReturnPct: field("grossReturnPct", "gross_return_pct", field("grossReturn", "gross_return")),
    netReturnPct: field("netReturnPct", "net_return_pct", field("netReturn", "net_return")),
    realizedPnl: field("realizedPnl", "realized_pnl"),
    rMultiple: field("rMultiple", "r_multiple"),
    holdingMinutes: field("holdingMinutes", "holding_minutes"),
    dataQualityStatus: field("dataQualityStatus", "data_quality_status"),
    updatedAt: field("updatedAt", "updated_at"),
  });
}

function normalizeA4SignalLifecycleCounts(value: unknown): JsonValue {
  const source = record(value);
  if (!source) return {};
  const counts: Record<string, number> = {};
  for (const [key, raw] of Object.entries(source)) {
    if (typeof raw !== "number" || !Number.isFinite(raw) || raw < 0) continue;
    const normalizedKey = key.trim().toUpperCase();
    if (!normalizedKey) continue;
    counts[normalizedKey] = Math.floor(raw);
  }
  return sanitizeJson(counts);
}

function normalizeNotification(value: unknown): JsonValue | null {
  const notification = record(value);
  if (!notification) return null;
  return sanitizeJson({
    deliveryId: notification.deliveryId ?? notification.delivery_id ?? null,
    kind: notification.kind ?? null,
    sourceId: notification.sourceId ?? notification.source_id ?? null,
    status: notification.status ?? null,
    title: notification.title ?? null,
    color: notification.color ?? null,
    attemptCount: notification.attemptCount ?? notification.attempt_count ?? 0,
    lastReasonCode: notification.lastReasonCode ?? notification.last_reason_code ?? null,
    createdAt: notification.createdAt ?? notification.created_at ?? null,
    updatedAt: notification.updatedAt ?? notification.updated_at ?? null,
    sentAt: notification.sentAt ?? notification.sent_at ?? null,
  });
}

export function normalizeA5Review(value: unknown): JsonValue | null {
  const review = record(value);
  if (!review) return null;
  return sanitizeJson({
    reviewId: review.reviewId ?? review.review_id ?? null,
    tradeDate: review.tradeDate ?? review.trade_date ?? null,
    reviewKind: review.reviewKind ?? review.review_kind ?? null,
    cutoffAt: review.cutoffAt ?? review.cutoff_at ?? null,
    createdAt: review.createdAt ?? review.created_at ?? null,
    status: review.status ?? null,
    model: review.model ?? null,
    markdownPath: review.markdownPath ?? review.markdown_path ?? null,
    metrics: review.metrics ?? {},
    dataQuality: review.dataQuality ?? review.data_quality ?? {},
    report: review.report ?? {},
  });
}

const MONITOR_LOG_LIMIT = 1_000;
const SAFE_MONITOR_CODE = /^[A-Z][A-Z0-9_]{0,95}$/;
const SAFE_MONITOR_SYMBOL = /^\d{6}\.(?:SH|SZ|BJ)$/;

interface MonitorAttempt {
  readonly runId: string;
  startedAt: string | null;
  finishedAt: string | null;
  nodeStatus: "running" | "succeeded" | "failed" | "terminated" | "skipped" | null;
  pythonStatus: string | null;
  reasonCode: string | null;
  diagnosticCode: string | null;
  affectedPlanCount: number | null;
  affectedSymbols: Set<string>;
}

function safeMonitorCode(value: string | null): string | null {
  if (!value) return null;
  const normalized = value.trim().toUpperCase();
  return SAFE_MONITOR_CODE.test(normalized) ? normalized : null;
}

function safeMonitorSymbol(value: string | null): string | null {
  if (!value) return null;
  const normalized = value.trim().toUpperCase();
  return SAFE_MONITOR_SYMBOL.test(normalized) ? normalized : null;
}

function logJsonString(message: string, key: string): string | null {
  const match = new RegExp(`"${key}"\\s*:\\s*"([^"\\r\\n]{1,160})"`).exec(message);
  return match?.[1] ?? null;
}

function logJsonNumber(message: string, key: string): number | null {
  const match = new RegExp(`"${key}"\\s*:\\s*(-?\\d+(?:\\.\\d+)?)`).exec(message);
  if (!match) return null;
  const value = Number(match[1]);
  return Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function monitorStateFromLatest(
  latest: JsonRecord | null,
  activePlanCount: number,
): MonitorDispatchStatus {
  if (!latest) return "UNKNOWN";
  const explicitStatus = asString(latest.status)?.toUpperCase();
  if (explicitStatus === "FAILED") return "FAILED";
  if (explicitStatus === "DATA_BLOCK" || explicitStatus === "BLOCKED") return "DATA_BLOCK";
  if (explicitStatus === "EMPTY_SCOPE") return "EMPTY_SCOPE";
  if (explicitStatus === "EFFECTIVE_SIGNAL") return "EFFECTIVE_SIGNAL";
  if (explicitStatus === "SUCCEEDED_NO_ACTION" || explicitStatus === "NO_ACTION") return "SUCCEEDED_NO_ACTION";
  const lanes = arrayField(latest, "lanes");
  const events = lanes.flatMap((lane) => arrayField(lane, "events"));
  const eventRecords = events.map((event) => record(event)).filter((event): event is JsonRecord => event !== null);
  if (eventRecords.some((event) => event.effective === true && asString(event.action)?.toUpperCase() !== "EMPTY_SCOPE")) {
    return "EFFECTIVE_SIGNAL";
  }
  if (eventRecords.some((event) => {
    const action = asString(event.action)?.toUpperCase();
    const reason = asString(event.reason_code ?? event.reasonCode)?.toUpperCase() ?? "";
    return action === "DATA_BLOCK" || reason.startsWith("DATA_") || reason.startsWith("MINUTE_DATA_");
  }) || lanes.some((lane) => record(lane)?.blocked === true)) {
    return "DATA_BLOCK";
  }
  if (eventRecords.length > 0 && eventRecords.every((event) => asString(event.action)?.toUpperCase() === "EMPTY_SCOPE")) {
    return "EMPTY_SCOPE";
  }
  if (activePlanCount === 0 && eventRecords.length > 0) return "EMPTY_SCOPE";
  return "SUCCEEDED_NO_ACTION";
}

function attemptSortKey(attempt: MonitorAttempt): number {
  const value = attempt.finishedAt ?? attempt.startedAt;
  const timestamp = value ? Date.parse(value) : Number.NaN;
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function ensureMonitorAttempt(attempts: Map<string, MonitorAttempt>, runId: string): MonitorAttempt {
  const existing = attempts.get(runId);
  if (existing) return existing;
  const created: MonitorAttempt = {
    runId,
    startedAt: null,
    finishedAt: null,
    nodeStatus: null,
    pythonStatus: null,
    reasonCode: null,
    diagnosticCode: null,
    affectedPlanCount: null,
    affectedSymbols: new Set<string>(),
  };
  attempts.set(runId, created);
  return created;
}

function incorporateMonitorLog(attempts: Map<string, MonitorAttempt>, log: LogEvent): void {
  if (log.job !== "monitor" || !log.runId) return;
  const attempt = ensureMonitorAttempt(attempts, log.runId);
  if (log.message.includes("开始执行 run-monitor")) {
    if (!attempt.startedAt || log.timestamp < attempt.startedAt) attempt.startedAt = log.timestamp;
  }
  const end = /任务结束 run-monitor status=(running|succeeded|failed|terminated|skipped)\b/i.exec(log.message);
  if (end) {
    attempt.nodeStatus = end[1]?.toLowerCase() as MonitorAttempt["nodeStatus"];
    attempt.finishedAt = log.timestamp;
  }
  const pythonStatus = logJsonString(log.message, "status")?.toUpperCase();
  if (pythonStatus) attempt.pythonStatus = pythonStatus;
  const reason = safeMonitorCode(logJsonString(log.message, "reason_code"));
  if (reason) attempt.reasonCode = reason;
  const diagnostic = safeMonitorCode(logJsonString(log.message, "diagnostic_code"));
  if (diagnostic) attempt.diagnosticCode = diagnostic;
  const symbol = safeMonitorSymbol(logJsonString(log.message, "symbol"));
  if (symbol) attempt.affectedSymbols.add(symbol);
  const planCount = logJsonNumber(log.message, "plan_count");
  if (planCount !== null) attempt.affectedPlanCount = planCount;
}

function incorporateMonitorRun(attempts: Map<string, MonitorAttempt>, run: JobRunRecord): void {
  if (run.job !== "monitor") return;
  const attempt = ensureMonitorAttempt(attempts, run.runId);
  attempt.startedAt = run.startedAt;
  attempt.finishedAt = run.finishedAt;
  attempt.nodeStatus = run.status;
}

/**
 * Project monitor process logs and latest output into one stable business
 * status.  The latest JSON file contains successful monitor output only, so
 * a newer failed dispatch must take precedence and must not look like an
 * empty scope in the workbench.
 */
export function summarizeMonitorDispatch(input: {
  readonly latest: unknown;
  readonly logs?: readonly LogEvent[];
  readonly runs?: readonly JobRunRecord[];
  readonly activePlanCount?: number;
}): MonitorDispatchSummary {
  const latest = record(input.latest);
  const checkedAt = latest ? asString(latest.time) : null;
  const attempts = new Map<string, MonitorAttempt>();
  for (const log of input.logs ?? []) incorporateMonitorLog(attempts, log);
  for (const run of input.runs ?? []) incorporateMonitorRun(attempts, run);
  const ordered = [...attempts.values()].sort((left, right) => attemptSortKey(right) - attemptSortKey(left));
  const running = ordered.find((attempt) => attempt.nodeStatus === "running" || (attempt.startedAt !== null && attempt.finishedAt === null));
  const latestAttempt = ordered[0] ?? null;
  const latestOutputMs = checkedAt ? Date.parse(checkedAt) : Number.NaN;
  const latestAttemptMs = latestAttempt ? attemptSortKey(latestAttempt) : Number.NaN;
  const latestAttemptFailed = latestAttempt !== null
    && (latestAttempt.nodeStatus === "failed" || latestAttempt.nodeStatus === "terminated" || latestAttempt.pythonStatus === "FAILED")
    && (!Number.isFinite(latestOutputMs) || latestAttemptMs >= latestOutputMs);
  const latestState = monitorStateFromLatest(latest, input.activePlanCount ?? 0);
  const status: MonitorDispatchStatus = running
    ? "RUNNING"
    : latestAttemptFailed
      ? "FAILED"
      : latestState;
  const successfulAttempts = ordered.filter((attempt) => attempt.nodeStatus === "succeeded").length;
  const failedAttempts = ordered.filter((attempt) => attempt.nodeStatus === "failed" || attempt.nodeStatus === "terminated" || attempt.pythonStatus === "FAILED").length;
  const success = ordered.find((attempt) => attempt.nodeStatus === "succeeded");
  const failure = ordered.find((attempt) => attempt.nodeStatus === "failed" || attempt.nodeStatus === "terminated" || attempt.pythonStatus === "FAILED");
  const affectedSymbols = [...new Set((latestAttempt?.affectedSymbols ?? new Set<string>()).values())].sort();
  const affectedPlanCount = latestAttempt?.affectedPlanCount ?? (status === "FAILED" ? input.activePlanCount ?? null : null);
  return {
    status,
    checkedAt,
    latestRunId: latestAttempt?.runId ?? null,
    latestAttemptAt: latestAttempt?.startedAt ?? latestAttempt?.finishedAt ?? checkedAt,
    latestCompletedAt: latestAttempt?.finishedAt ?? null,
    lastSuccessAt: success?.finishedAt ?? null,
    lastFailureAt: failure?.finishedAt ?? null,
    lastReasonCode: (latestAttemptFailed ? latestAttempt?.reasonCode : null) ?? failure?.reasonCode ?? null,
    lastDiagnosticCode: (latestAttemptFailed ? latestAttempt?.diagnosticCode : null) ?? failure?.diagnosticCode ?? null,
    affectedPlanCount,
    affectedSymbols,
    attemptCount: ordered.length,
    successCount: successfulAttempts,
    failureCount: failedAttempts,
  };
}

export function normalizeA4Replay(value: unknown): JsonValue | null {
  const replay = record(value);
  if (!replay) return null;
  const plan = record(replay.test_plan);
  const coverage = record(replay.bar_coverage);
  const testPlan = plan ? {
    planId: plan.plan_id ?? null,
    symbol: plan.symbol ?? null,
    name: plan.name ?? null,
    status: "TEST_ONLY",
    setupType: plan.setup_type ?? null,
    expiresAt: plan.expires_at ?? null,
    riskUnit: plan.test_risk_unit ?? null,
    selectionReasons: plan.selection_reasons ?? [],
    sourcePool: plan.source_pool ?? null,
    sourceRiskUnit: plan.source_risk_unit ?? null,
    testRiskUnit: plan.test_risk_unit ?? null,
    triggerLow: plan.trigger_low ?? null,
    triggerHigh: plan.trigger_high ?? null,
    stopLevel: plan.stop_level ?? null,
    testOnlyPromotion: plan.test_only_promotion ?? null,
  } : null;
  const fillRows = arrayField(replay, "fills").map((fill) => record(fill)).filter((fill): fill is JsonRecord => fill !== null);
  const fills = fillRows.map((fill) => normalizeSimulation({ ...fill, status: "FILLED" })).filter(Boolean);
  const effectiveEvents = arrayField(replay, "effective_events").map((event) => {
    const normalized = record(normalizeMonitorEvent(event)) ?? {};
    const action = asString(normalized.action);
    const fill = action && ["BUY_SIGNAL", "ADD_SIGNAL", "SELL_SIGNAL", "REDUCE_SIGNAL", "FORCED_RISK_EXIT"].includes(action)
      ? fillRows.find((item) => item.symbol === normalized.symbol)
      : null;
    return {
      ...normalized,
      testOnly: true,
      plan: testPlan,
      simulation: fill ? normalizeSimulation({ ...fill, status: "FILLED" }) : null,
    };
  });
  return sanitizeJson({
    schemaVersion: replay.schema_version ?? null,
    status: replay.status ?? null,
    mode: replay.mode ?? null,
    modelMode: replay.model_mode ?? null,
    tradeDate: replay.trade_date ?? null,
    sourceRunId: replay.source_run_id ?? null,
    officialA3PlanCount: replay.official_a3_plan_count ?? null,
    productionPathExpected: replay.production_path_expected ?? null,
    modelCalls: replay.model_calls ?? null,
    testPlan,
    barCoverage: coverage ? {
      source: coverage.source ?? null,
      count: coverage.count ?? null,
      first: coverage.first ?? null,
      last: coverage.last ?? null,
    } : null,
    effectiveEvents,
    fills,
    invariants: replay.invariants ?? null,
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
    poolCounts: normalizeStagePoolCounts(stage),
    latencyMs: typeof latency === "number" && Number.isFinite(latency) ? latency : null,
    reasonCodes,
    outcome: normalizeStageOutcome(stage, asString(stage.stage) ?? "UNKNOWN"),
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

function laneOverview(
  laneId: string,
  rows: readonly JsonRecord[],
  payload: JsonRecord | null,
  disabled = false,
): JsonValue {
  if (disabled) {
    return sanitizeJson({
      laneId,
      model: null,
      status: "DISABLED",
      runId: null,
      slot: null,
      stages: [],
      reasonCodes: ["COMPARISON_DISABLED"],
      outcome: null,
      source: "configuration",
    });
  }
  const row = rows.find((item) => item.lane_id === laneId) ?? null;
  const payloadLane = payload && stringField(payload, "lane") === laneId ? payload : null;
  const stages = payloadLane
    ? arrayField(payloadLane, "stages").map((stage) => normalizeStage(stage))
    : null;
  const model = row ? asString(row.model) : payloadLane ? asString(payloadLane.model) : null;
  const stageStatuses = row ? parseJsonArray(row.reason_codes_json) : null;
  const storedOutcome = row ? parseJsonObject(row.outcome_json) : null;
  const outcome = normalizeLaneOutcome(
    payloadLane ?? (storedOutcome ? { ...row, outcome_v2: storedOutcome } : row),
    laneId,
    model,
  );
  return sanitizeJson({
    laneId,
    model,
    status: row ? asString(row.status) : payloadLane ? asString(payloadLane.status) : null,
    runId: row ? asString(row.run_id) : null,
    slot: row ? asString(row.slot) : null,
    stages,
    reasonCodes: stageStatuses,
    outcome,
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
    const currentRows = rows.filter((item) => asString(item.run_id) === runId);
    const monitor = await this.files.monitor();
    const workflowProgress = await this.files.workflowProgress();
    const dataSources = (await this.files.dataSources()).map((source) => ({
      ...source,
      id: source.provider,
      label: source.provider,
      checkedAt: source.generatedAt,
      detail: source.path,
    }));
    const statusData = status.data;
    const acceptance = record(statusData?.latest_workflow_acceptance);
    const storedRunOutcome = record(statusData?.latest_workflow_outcome_v2);
    const latestRun = runs.find((item) => item.runId === runId) ?? null;
    const lanes = ["lane_1", "lane_2", "lane_3"].map((laneId) => laneOverview(
      laneId,
      currentRows,
      laneRecords.find((item) => item.lane === laneId) ?? null,
      !this.config.comparisonEnabled && laneId !== this.config.researchPrimaryLaneId,
    ));
    const runOutcome = normalizeRunOutcome(
      storedRunOutcome ?? (acceptance ? { ...acceptance, lanes } : { run_id: runId, lanes }),
    );
    const latestWorkflow = runId
      ? sanitizeJson({
        runId,
        slot: currentRows[0] ? asString(currentRows[0].slot) : latestRun?.slot ?? null,
        status: runOutcome?.legacy_status ?? (acceptance ? asString(acceptance.status) : latestRun?.status ?? null),
        tradeDate: currentRows[0] ? asString(currentRows[0].trade_date) : null,
        snapshotHash: currentRows[0] ? asString(currentRows[0].snapshot_hash) : null,
        createdAt: currentRows[0] ? asString(currentRows[0].created_at) : null,
        updatedAt: currentRows[0] ? asString(currentRows[0].updated_at) : null,
        researchMarkdown: latestRun?.researchMarkdown ?? null,
        acceptance: acceptance ? sanitizeJson(acceptance) : null,
        outcome: runOutcome,
        lanes,
      })
      : null;
    const monitorRecord = record(monitor.latest);
    const monitorLanes = monitorRecord ? arrayField(monitorRecord, "lanes") : [];
    const scheduleSnapshot = this.scheduler.snapshot();
    const accounts = statusData ? normalizeAccounts(statusData) : null;
    const planCounts = statusData ? statusData.plan_counts ?? null : null;
    const monitorEvents = monitorRecord
      ? monitorLanes.flatMap((lane) => arrayField(lane, "events").map((event) => normalizeMonitorEvent(event)))
      : [];
    const persistedEffectiveEvents = statusData ? arrayField(statusData, "recent_effective_events") : [];
    const effectiveEvents = (persistedEffectiveEvents.length ? persistedEffectiveEvents : monitor.events)
      .map((event) => normalizeMonitorEvent(event));
    const monitorPlans = statusData
      ? arrayField(statusData, "monitor_plans").map((plan) => normalizeMonitorPlan(plan)).filter(Boolean)
      : [];
    const recentNotifications = statusData
      ? arrayField(statusData, "recent_notifications").map((item) => normalizeNotification(item)).filter(Boolean)
      : [];
    const recentA5Reviews = statusData
      ? arrayField(statusData, "recent_a5_reviews").map((item) => normalizeA5Review(item)).filter(Boolean)
      : [];
    const monitorPlanRecords = monitorPlans
      .map((plan) => record(plan))
      .filter((plan): plan is JsonRecord => plan !== null);
    const activePlans = monitorPlanRecords.filter((plan) => asString(plan.status) === "ACTIVE_TODAY");
    const pendingPlans = monitorPlanRecords.filter((plan) => asString(plan.status) === "PENDING_MORNING_REVIEW");
    // Newer Python status payloads may provide a complete latest A3 plan
    // projection.  Keep the fallback to the existing monitor_plans field so
    // old deployments remain readable during a rolling update.
    const latestA3Raw = statusData
      ? arrayField(statusData, "latest_a3_plans").length
        ? arrayField(statusData, "latest_a3_plans")
        : arrayField(statusData, "latest_published_a3_plans").length
          ? arrayField(statusData, "latest_published_a3_plans")
          : monitorPlans
      : [];
    const latestA3Plans = latestA3Raw.map((plan) => normalizeMonitorPlan(plan)).filter(Boolean);
    const monitorLogs = await this.logger.list(MONITOR_LOG_LIMIT, undefined, "monitor");
    const activePlanCount = planCount(statusData, "ACTIVE_TODAY") ?? activePlans.length;
    const pendingPlanCount = planCount(statusData, "PENDING_MORNING_REVIEW") ?? pendingPlans.length;
    const monitorDispatch = summarizeMonitorDispatch({
      latest: monitor.latest,
      logs: monitorLogs,
      runs: this.runner.recentRuns(MONITOR_LOG_LIMIT),
      activePlanCount,
    });
    const latestA3RunId = statusData
      ? asString(statusData.latest_a3_run_id)
        ?? asString(statusData.latest_published_a3_run_id)
        ?? (record(latestA3Plans[0]) ? asString(record(latestA3Plans[0])?.sourceRunId) : null)
      : null;
    const latestA3PublishedAt = statusData
      ? asString(statusData.latest_a3_published_at) ?? asString(statusData.latest_published_a3_at)
      : null;
    const signalLifecycles = statusData
      ? arrayField(statusData, "recent_a4_signal_lifecycles")
        .map((item) => normalizeA4SignalLifecycle(item))
        .filter(Boolean)
      : [];
    const signalLifecycleCounts = statusData
      ? normalizeA4SignalLifecycleCounts(statusData.a4_signal_lifecycle_counts)
      : {};
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
          latestOutcome: runOutcome,
          a1Generation: statusData ? statusData.a1_generation ?? null : null,
        }
        : null,
      a1Generation: statusData ? statusData.a1_generation ?? null : null,
      latestWorkflow,
      workflowProgress,
      lanes,
      monitor: {
        status: monitorDispatch.status,
        checkedAt: monitorRecord ? asString(monitorRecord.time) : null,
        effectiveEventCount: monitor.events.length,
        activePlanCount,
        pendingPlanCount,
        events: monitorEvents,
        latest: monitor.latest,
        effectiveSignals: monitor.effectiveSignals,
        laneCount: monitorLanes.length,
        plans: monitorPlans,
        activePlans,
        pendingPlans,
        latestA3Plans,
        latestA3RunId,
        latestA3PublishedAt,
        dispatch: monitorDispatch,
        replay: normalizeA4Replay(monitor.replay),
        notifications: recentNotifications,
        signalLifecycles,
        signalLifecycleCounts,
      },
      accounts,
      positions: statusData ? statusData.positions ?? null : null,
      plans: planCounts,
      planCounts,
      fills: statusData ? statusData.fills ?? null : null,
      dataSources,
      recentEffectiveEvents: effectiveEvents,
      recentA5Reviews,
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

  public async stageDetail(
    runId: string,
    laneId: string,
    stage: string,
    pool: string,
    page: number,
    pageSize: number,
    query: string,
    reason: string,
  ): Promise<JsonValue | null> {
    const detail = await this.files.researchStageDetail(runId, laneId, stage, pool, page, pageSize, query, reason);
    return detail ? sanitizeJson(detail) : null;
  }

  public async logs(limit: number, level?: "debug" | "info" | "warn" | "error", job?: string): Promise<JsonValue> {
    return sanitizeJson({ logs: await this.logger.list(limit, level, job), limit, level: level ?? null, job: job ?? null });
  }
}
