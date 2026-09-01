import type { LogEntry, OverviewResponse, StageOutcomeContract } from "./types";

export type WorkbenchIssueSeverity = "CRITICAL" | "WARNING" | "INFO";
export type WorkbenchIssueStatus = "OPEN" | "OBSERVING";

export interface WorkbenchIssue {
  id: string;
  severity: WorkbenchIssueSeverity;
  status: WorkbenchIssueStatus;
  source: "DEPLOYMENT" | "WORKFLOW" | "DATA_SOURCE" | "RUNTIME" | "PLAN";
  code: string;
  title: string;
  detail: string;
  runId?: string | null;
  laneId?: string | null;
  stage?: string | null;
  firstSeenAt?: string | null;
  lastSeenAt?: string | null;
  occurrenceCount: number;
}

const CODE_TITLES: Record<string, string> = {
  LATEST_WORKFLOW_NOT_READY: "最近工作流尚未就绪",
  STATE_DB_UNHEALTHY: "状态库异常",
  CONFIGURATION_NOT_READY: "运行配置未就绪",
  DEPLOYMENT_NOT_READY: "部署门禁未通过",
  DATA_COVERAGE_INSUFFICIENT: "阶段事实覆盖不足",
  DATA_GAP: "阶段存在事实缺口",
  HEARTBEAT_TIMEOUT: "任务进度心跳超时",
  INVALID_JSON: "进度文件格式错误",
  INVALID_SHAPE: "进度文件结构不兼容",
  OVERSIZE: "进度文件超过读取上限",
  UNREADABLE: "进度文件无法读取",
  NO_ACTIVE_A3_PLAN: "当前没有可执行 A3 计划",
  A4_DISPATCH_FAILED: "A4 盘中调度失败",
  A4_DATA_BLOCK: "A4 盘中数据阻断",
};

function token(value: unknown, fallback: string): string {
  const normalized = String(value ?? "").trim().toUpperCase().replace(/[^A-Z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return normalized || fallback;
}

function titleFor(code: string, fallback: string): string {
  return CODE_TITLES[code] ?? fallback;
}

function issueId(parts: Array<string | null | undefined>): string {
  return parts.map((part) => token(part, "NA")).join(":");
}

function stageSeverity(outcome: StageOutcomeContract): WorkbenchIssueSeverity {
  return outcome.quality_state === "FAILED" || outcome.job_status === "FAILED" ? "CRITICAL" : "WARNING";
}

function stageDetail(outcome: StageOutcomeContract): string {
  const coverage = outcome.data_coverage;
  const coverageText = typeof coverage?.actual === "number" || typeof coverage?.required === "number"
    ? `；事实覆盖 ${coverage.actual ?? "—"}/${coverage.required ?? "—"}`
    : "";
  return `质量=${outcome.quality_state}，数据充分性=${outcome.data_sufficiency_state}，发布=${outcome.publication_state}${coverageText}`;
}

function isHealthyDataSource(status: string): boolean {
  return ["HEALTHY", "READY", "OK", "PASS", "ACTIVE", "VALIDATED"].includes(token(status, "UNKNOWN"));
}

function runtimeKey(log: LogEntry): string {
  return `${token(log.job, "GENERAL")}:${token(log.message.replace(/\b[0-9a-f]{8,}\b/gi, "ID").replace(/\b\d{2,}\b/g, "N"), "RUNTIME_EVENT")}`;
}

/** Build a safe, bounded issue projection from durable status plus recent logs. */
export function collectWorkbenchIssues(overview: OverviewResponse, logs: LogEntry[]): WorkbenchIssue[] {
  const issues: WorkbenchIssue[] = [];
  const runId = overview.latestWorkflow.runId ?? null;

  for (const blocker of overview.service.blockers ?? []) {
    const code = token(blocker, "DEPLOYMENT_BLOCKER");
    issues.push({
      id: issueId(["deployment", code]), severity: "CRITICAL", status: "OPEN", source: "DEPLOYMENT",
      code, title: titleFor(code, "部署门禁存在阻断"), detail: blocker, occurrenceCount: 1,
    });
  }
  if (overview.service.stateHealthy === false) {
    issues.push({ id: "DEPLOYMENT:STATE_DB_UNHEALTHY", severity: "CRITICAL", status: "OPEN", source: "DEPLOYMENT", code: "STATE_DB_UNHEALTHY", title: CODE_TITLES.STATE_DB_UNHEALTHY, detail: "Node 服务报告状态库不可用或完整性检查未通过。", occurrenceCount: 1 });
  }
  if (overview.service.configurationReady === false) {
    issues.push({ id: "DEPLOYMENT:CONFIGURATION_NOT_READY", severity: "CRITICAL", status: "OPEN", source: "DEPLOYMENT", code: "CONFIGURATION_NOT_READY", title: CODE_TITLES.CONFIGURATION_NOT_READY, detail: "生产运行所需配置尚未全部就绪。", occurrenceCount: 1 });
  }
  if (overview.service.deploymentReady === false) {
    issues.push({ id: "DEPLOYMENT:DEPLOYMENT_NOT_READY", severity: "WARNING", status: "OPEN", source: "DEPLOYMENT", code: "DEPLOYMENT_NOT_READY", title: CODE_TITLES.DEPLOYMENT_NOT_READY, detail: "至少一项部署门禁仍未通过。", occurrenceCount: 1 });
  }

  const progressIssue = overview.workflowProgress?.issue ?? overview.workflowProgress?.staleIssue;
  if (progressIssue || overview.workflowProgress?.stale) {
    const code = token(progressIssue, "HEARTBEAT_TIMEOUT");
    issues.push({
      id: issueId(["workflow", runId, code]), severity: "WARNING", status: "OPEN", source: "WORKFLOW", code,
      title: titleFor(code, "工作流进度不可用"), detail: "进度心跳或进度文件异常；请结合运行日志确认任务是否仍在推进。",
      runId, lastSeenAt: overview.workflowProgress?.updatedAt, occurrenceCount: 1,
    });
  }

  for (const lane of overview.latestWorkflow.lanes) {
    for (const stage of lane.stages) {
      const outcome = stage.outcome;
      if (!outcome || outcome.lifecycle_state !== "TERMINAL") continue;
      const marketUnderfilled = outcome.data_sufficiency_state === "SUFFICIENT"
        && outcome.opportunity_state !== "UNKNOWN"
        && outcome.reason_codes.some((reason) => reason.includes("UNDERFILLED_MARKET"));
      if (marketUnderfilled) {
        const selected = outcome.counts.selected ?? stage.symbolCount ?? 0;
        issues.push({
          id: issueId(["workflow", runId, lane.laneId, stage.stage, "MARKET_OPPORTUNITY_UNDERFILLED"]), severity: "INFO", status: "OBSERVING", source: "WORKFLOW",
          code: "MARKET_OPPORTUNITY_UNDERFILLED", title: `${stage.stage} 市场机会少于目标数量`,
          detail: `事实覆盖充分，当前筛选得到 ${selected} 只；这是市场截面结果，不是数据或执行故障。`, runId,
          laneId: lane.laneId, stage: stage.stage, lastSeenAt: overview.latestWorkflow.updatedAt, occurrenceCount: 1,
        });
        continue;
      }
      const hasQualityIssue = ["FAILED", "BLOCKED", "DEGRADED"].includes(outcome.quality_state);
      const hasDataIssue = outcome.data_sufficiency_state === "INSUFFICIENT"
        || (outcome.data_sufficiency_state === "PARTIAL" && outcome.opportunity_state === "UNKNOWN");
      if (!hasQualityIssue && !hasDataIssue) continue;
      // A validated empty opportunity is a legitimate research result, not a fault.
      if (outcome.quality_state === "VALIDATED" && outcome.opportunity_state === "ABSENT") continue;
      const code = token(outcome.reason_codes[0], hasDataIssue ? "DATA_COVERAGE_INSUFFICIENT" : `${stage.stage}_STAGE_${outcome.quality_state}`);
      issues.push({
        id: issueId(["workflow", runId, lane.laneId, stage.stage, code]), severity: stageSeverity(outcome), status: "OPEN", source: "WORKFLOW",
        code, title: `${stage.stage} ${titleFor(code, "阶段未通过验收")}`, detail: stageDetail(outcome), runId,
        laneId: lane.laneId, stage: stage.stage, lastSeenAt: overview.latestWorkflow.updatedAt, occurrenceCount: 1,
      });
    }
  }

  for (const source of overview.dataSources) {
    if (isHealthyDataSource(source.status)) continue;
    const code = `DATA_SOURCE_${token(source.id, "UNKNOWN")}_${token(source.status, "UNKNOWN")}`;
    issues.push({
      id: issueId(["data", source.id]), severity: token(source.status, "UNKNOWN").includes("FAIL") || token(source.status, "UNKNOWN").includes("BLOCK") ? "CRITICAL" : "WARNING",
      status: "OPEN", source: "DATA_SOURCE", code, title: `${source.label} 数据源${source.status || "状态未知"}`,
      detail: source.detail || "数据源未报告可用状态。", lastSeenAt: source.checkedAt, occurrenceCount: 1,
    });
  }

  const monitorDispatch = overview.monitor.dispatch;
  if (monitorDispatch?.status === "FAILED" || monitorDispatch?.status === "DATA_BLOCK") {
    const isFailure = monitorDispatch.status === "FAILED";
    const code = token(monitorDispatch.lastReasonCode, isFailure ? "A4_DISPATCH_FAILED" : "A4_DATA_BLOCK");
    const symbols = monitorDispatch.affectedSymbols?.length
      ? `；影响标的 ${monitorDispatch.affectedSymbols.slice(0, 8).join("、")}`
      : "";
    const count = typeof monitorDispatch.affectedPlanCount === "number"
      ? `；影响计划 ${monitorDispatch.affectedPlanCount} 个`
      : "";
    issues.push({
      id: issueId(["runtime", "monitor", code]),
      severity: isFailure ? "CRITICAL" : "WARNING",
      status: "OPEN",
      source: "RUNTIME",
      code,
      title: titleFor(code, isFailure ? "A4 盘中调度失败" : "A4 盘中数据阻断"),
      detail: `最近一次 A4 调度${isFailure ? "未完成" : "未通过数据门禁"}；${monitorDispatch.lastDiagnosticCode ? `诊断码 ${monitorDispatch.lastDiagnosticCode}` : "未提供诊断码"}${symbols}${count}。系统不会把该状态误记为 EMPTY_SCOPE。`,
      runId: monitorDispatch.latestRunId,
      lastSeenAt: monitorDispatch.lastFailureAt ?? monitorDispatch.latestCompletedAt,
      occurrenceCount: Math.max(1, monitorDispatch.failureCount ?? 1),
    });
  }

  const planCount = Object.values(overview.planCounts).reduce((sum, count) => sum + (Number.isFinite(count) ? count : 0), 0);
  if (!overview.activeJob && planCount === 0) {
    issues.push({
      id: "PLAN:NO_ACTIVE_A3_PLAN", severity: "INFO", status: "OBSERVING", source: "PLAN", code: "NO_ACTIVE_A3_PLAN",
      title: CODE_TITLES.NO_ACTIVE_A3_PLAN, detail: "这不是运行故障；盘中会保持全市场观察，但不会发布模拟入场信号。", runId, occurrenceCount: 1,
    });
  }

  const runtime = new Map<string, WorkbenchIssue>();
  for (const log of logs) {
    const level = token(log.level, "INFO");
    if (!level.includes("ERROR") && !level.includes("WARN") && !level.includes("FATAL")) continue;
    const key = runtimeKey(log);
    const current = runtime.get(key);
    if (current) {
      current.occurrenceCount += 1;
      if (!current.firstSeenAt || log.timestamp < current.firstSeenAt) current.firstSeenAt = log.timestamp;
      if (!current.lastSeenAt || log.timestamp > current.lastSeenAt) current.lastSeenAt = log.timestamp;
      continue;
    }
    const code = token(log.message.slice(0, 96), "RUNTIME_EVENT");
    runtime.set(key, {
      id: issueId(["runtime", key]), severity: level.includes("ERROR") || level.includes("FATAL") ? "CRITICAL" : "WARNING",
      status: "OBSERVING", source: "RUNTIME", code, title: `${log.job || "运行时"}近期${level.includes("WARN") ? "警告" : "错误"}`,
      detail: log.message, runId: log.runId, firstSeenAt: log.timestamp, lastSeenAt: log.timestamp, occurrenceCount: 1,
    });
  }
  issues.push(...runtime.values());

  const rank: Record<WorkbenchIssueSeverity, number> = { CRITICAL: 0, WARNING: 1, INFO: 2 };
  return issues.sort((left, right) => rank[left.severity] - rank[right.severity]
    || Number(left.status === "OBSERVING") - Number(right.status === "OBSERVING")
    || String(right.lastSeenAt ?? "").localeCompare(String(left.lastSeenAt ?? "")));
}
