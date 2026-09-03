import {
  Activity,
  CalendarClock,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  CircleAlert,
  CircleDotDashed,
  Database,
  FileClock,
  Filter,
  GitBranch,
  Gauge,
  KeyRound,
  LayoutDashboard,
  Menu,
  MonitorDot,
  RefreshCw,
  Search,
  ScrollText,
  Send,
  Server,
  ShieldCheck,
  TriangleAlert,
  WalletCards,
  X,
  XCircle,
} from "lucide-react";
import { FormEvent, KeyboardEvent, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { apiFetch, apiMutation, getStoredToken, saveToken, withQuery } from "./api";
import { collectWorkbenchIssues, WorkbenchIssue, WorkbenchIssueSeverity } from "./issues";
import {
  codeLabel,
  displayValue,
  fieldLabel,
  humanizeText,
  jobLabel,
  logLevelLabel,
  modelNameLabel,
  planPriorityText,
  slotLabel,
  stockSymbolLabel,
} from "./localization";
import {
  AccountSummary,
  A4SignalLifecycle,
  A5DailyReview,
  ApiError,
  DataSourceSummary,
  EffectiveEvent,
  HealthTone,
  LaneOutcomeContract,
  LaneSummary,
  LogEntry,
  LarkNotificationSummary,
  LarkSettingsStatus,
  LogsResponse,
  MonitorPlan,
  MonitorDispatchSummary,
  OverviewResponse,
  RunSummary,
  RunsResponse,
  RunOutcomeContract,
  readLaneOutcome,
  readRunOutcome,
  readStageOutcome,
  StageSummary,
  StageOutcomeContract,
  StageDetailItem,
  StageDetailPool,
  StageDetailResponse,
  StagePoolId,
  WorkflowProgressLane,
  WorkflowProgressStage,
  WorkflowProgressSummary,
} from "./types";

type ViewId = "overview" | "funnel" | "monitor" | "review" | "accounts" | "issues" | "logs" | "deployment";

const NAVIGATION: Array<{ id: ViewId; label: string; icon: typeof LayoutDashboard }> = [
  { id: "overview", label: "总览", icon: LayoutDashboard },
  { id: "funnel", label: "研究漏斗", icon: GitBranch },
  { id: "monitor", label: "盘中盯盘", icon: MonitorDot },
  { id: "review", label: "复盘评估", icon: Gauge },
  { id: "accounts", label: "模拟账户", icon: WalletCards },
  { id: "issues", label: "问题跟踪", icon: CircleAlert },
  { id: "logs", label: "运行日志", icon: ScrollText },
  { id: "deployment", label: "部署状态", icon: Server },
];

const MODEL_LABELS: Record<string, string> = {
  lane_1: "深度求索",
  lane_2: "月之暗面",
  lane_3: "智谱",
};

const STAGE_LABELS: Record<string, string> = {
  A1: "A1 基本面",
  A2: "A2 主题情绪",
  A3: "A3 技术计划",
};

const STRATEGY_PROFILE_LABELS: Record<string, string> = {
  LEADER_INTRADAY: "龙头战法",
  MA520_SWING: "520 均线波段",
  TREND_MA5: "趋势 5 日线",
};

const ELIGIBILITY_LABELS: Record<string, string> = {
  QUALIFIED: "可形成计划",
  WATCH_ONLY: "仅观察",
  DATA_GAP: "数据不足",
  REJECTED: "不符合",
};

const EMPTY_OVERVIEW: OverviewResponse = {
  generatedAt: "",
  service: { status: "unknown" },
  schedule: [],
  latestWorkflow: { status: "unknown", lanes: [] },
  workflowProgress: null,
  monitor: { status: "unknown", events: [], signalLifecycles: [], signalLifecycleCounts: {} },
  accounts: [],
  planCounts: {},
  dataSources: [],
  recentEffectiveEvents: [],
  recentA5Reviews: [],
  recentLogs: [],
};

function normalizeOverview(value: OverviewResponse): OverviewResponse {
  const rawWorkflow = value.latestWorkflow ?? {} as OverviewResponse["latestWorkflow"];
  const workflowOutcome = readRunOutcome(rawWorkflow.outcome ?? rawWorkflow.outcome_v2 ?? rawWorkflow.acceptance);
  const rawLanes = Array.isArray(rawWorkflow.lanes) ? rawWorkflow.lanes : [];
  const normalizedLanes = rawLanes.map((lane) => {
    const outcome = readLaneOutcome(lane.outcome ?? lane.outcome_v2 ?? lane);
    const rawStages = Array.isArray(lane.stages) ? lane.stages : [];
    const stages = rawStages.map((stage) => ({
      ...stage,
      outcome: readStageOutcome(stage.outcome ?? stage.outcome_v2 ?? stage)
        ?? outcome?.stages.find((candidate) => candidate.stage.toUpperCase() === stage.stage.toUpperCase())
        ?? null,
    }));
    return { ...lane, outcome, stages };
  });
  return {
    ...EMPTY_OVERVIEW,
    ...value,
    service: { ...EMPTY_OVERVIEW.service, ...(value.service ?? {}) },
    schedule: Array.isArray(value.schedule) ? value.schedule : [],
    latestWorkflow: {
      ...EMPTY_OVERVIEW.latestWorkflow,
      ...rawWorkflow,
      outcome: workflowOutcome,
      acceptance: readRunOutcome(rawWorkflow.acceptance ?? rawWorkflow.outcome ?? rawWorkflow.outcome_v2),
      lanes: normalizedLanes,
    },
    workflowProgress: value.workflowProgress && typeof value.workflowProgress === "object"
      ? {
        ...value.workflowProgress,
        reasonCode: value.workflowProgress.reasonCode ?? null,
        issue: value.workflowProgress.issue ?? null,
        stale: value.workflowProgress.stale === true,
        staleIssue: value.workflowProgress.staleIssue ?? null,
        lanes: Array.isArray(value.workflowProgress.lanes) ? value.workflowProgress.lanes : [],
        resources: value.workflowProgress.resources ?? null,
      }
      : null,
    monitor: {
      ...EMPTY_OVERVIEW.monitor,
      ...(value.monitor ?? {}),
      events: Array.isArray(value.monitor?.events) ? value.monitor.events : [],
      plans: Array.isArray(value.monitor?.plans) ? value.monitor.plans : [],
      activePlans: Array.isArray(value.monitor?.activePlans) ? value.monitor.activePlans : [],
      pendingPlans: Array.isArray(value.monitor?.pendingPlans) ? value.monitor.pendingPlans : [],
      latestA3Plans: Array.isArray(value.monitor?.latestA3Plans) ? value.monitor.latestA3Plans : [],
      notifications: Array.isArray(value.monitor?.notifications) ? value.monitor.notifications : [],
      dispatch: value.monitor?.dispatch ?? null,
      signalLifecycles: Array.isArray(value.monitor?.signalLifecycles) ? value.monitor.signalLifecycles : [],
      signalLifecycleCounts: value.monitor?.signalLifecycleCounts && typeof value.monitor.signalLifecycleCounts === "object"
        ? value.monitor.signalLifecycleCounts
        : {},
    },
    accounts: Array.isArray(value.accounts) ? value.accounts : [],
    planCounts: value.planCounts ?? {},
    dataSources: Array.isArray(value.dataSources) ? value.dataSources : [],
    recentEffectiveEvents: Array.isArray(value.recentEffectiveEvents) ? value.recentEffectiveEvents : [],
    recentA5Reviews: Array.isArray(value.recentA5Reviews) ? value.recentA5Reviews : [],
    recentLogs: Array.isArray(value.recentLogs) ? value.recentLogs : [],
  };
}

type OutcomeStatus = StageOutcomeContract | LaneOutcomeContract | RunOutcomeContract;

const DATA_GAP_REASONS = new Set([
  "DATA_GAP",
  "A2_DATA_GAP",
  "A2_CRITICAL_DATA_INSUFFICIENT",
  "A2_FACTOR_COVERAGE_BELOW_MINIMUM",
  "DATA_COVERAGE_INSUFFICIENT",
  "EVIDENCE_GAP",
  "TECHNICAL_DATA_UNAVAILABLE",
  "BLOCKED_TECHNICAL_DATA",
]);
const UPSTREAM_REASONS = new Set(["UPSTREAM_STAGE_BLOCKED", "UPSTREAM_POOL_EMPTY", "PRIMARY_LANE_MISSING"]);

/** Convert the backend's four axes to a display vocabulary. Counts are never used to infer this. */
function statusFromOutcome(outcome: OutcomeStatus): string {
  if (outcome.job_status === "STALE") return "STALE";
  if (outcome.job_status === "RUNNING" || outcome.lifecycle_state === "RUNNING") return "RUNNING";
  if (outcome.job_status === "QUEUED" || outcome.lifecycle_state === "QUEUED") return "QUEUED";
  const reasons = new Set(outcome.reason_codes);
  if (outcome.quality_state === "FAILED") return "TECHNICAL_FAILURE";
  if (outcome.quality_state === "CANCELLED") return "CANCELLED";
  if (outcome.quality_state === "BLOCKED") {
    if ([...reasons].some((reason) => DATA_GAP_REASONS.has(reason))) return "DATA_INSUFFICIENT";
    if ([...reasons].some((reason) => UPSTREAM_REASONS.has(reason))) return "UPSTREAM_NOT_RUN";
    return "BLOCKED";
  }
  if (outcome.quality_state === "DEGRADED") {
    return outcome.data_sufficiency_state === "INSUFFICIENT" || [...reasons].some((reason) => DATA_GAP_REASONS.has(reason)) ? "DATA_INSUFFICIENT" : "READY_DEGRADED";
  }
  const stage = "stage" in outcome ? outcome.stage.toUpperCase() : null;
  const opportunity = stage === "A1"
    ? outcome.research_opportunity_state
    : stage === "A2"
      ? outcome.focus_opportunity_state
      : stage === "A3"
        ? (outcome.actionability_state === "ACTIONABLE" ? "PRESENT" : outcome.actionability_state === "NO_ACTION" ? "ABSENT" : outcome.actionability_state === "UNKNOWN" ? "UNKNOWN" : "NOT_APPLICABLE")
        : outcome.actionability_state === "ACTIONABLE"
          ? "PRESENT"
          : outcome.focus_opportunity_state !== "NOT_APPLICABLE"
            ? outcome.focus_opportunity_state
            : outcome.research_opportunity_state;
  if (opportunity === "ABSENT") return stage === "A3" ? "VALIDATED_NO_ACTION" : stage === "A2" ? "VALIDATED_NO_OPPORTUNITY" : "VALIDATED_NO_OPPORTUNITY";
  if (opportunity === "UNKNOWN") return "DATA_INSUFFICIENT";
  if (outcome.publication_state === "PUBLISHED") return "PUBLISHED";
  if (outcome.publication_state === "READY") return "READY";
  return outcome.legacy_status || "VALIDATED";
}

function toneForStatus(status?: string | null): HealthTone {
  const normalized = (status ?? "").toUpperCase();
  if (["OK", "PASS", "HEALTHY", "READY", "READY_TO_PUBLISH", "PUBLISHED", "NOOP", "NOOP_NO_DIRTY", "COMPLETED", "VALIDATED", "VALIDATED_NO_OPPORTUNITY", "VALIDATED_NO_ACTION", "VALIDATED_NO_SETUP", "ACTIVE"].includes(normalized)) return "healthy";
  if (["RUNNING", "QUEUED", "IN_PROGRESS", "STARTED", "RETRYING"].includes(normalized)) return "running";
  if (["WARN", "WARNING", "DEGRADED", "READY_DEGRADED", "VALIDATED_UNDERFILLED_MARKET", "DEGRADED_UNDERFILLED_DATA_GAP", "NOT_RUN_UPSTREAM_BLOCKED", "UPSTREAM_NOT_RUN", "DATA_INSUFFICIENT", "BLOCKED", "BLOCKED_SOURCE_GENERATION", "BLOCKED_DATA_COVERAGE", "BLOCKED_EVIDENCE_GAP", "BLOCKED_MODEL", "BLOCKED_TECHNICAL_DATA", "PARTIAL", "MISSED", "STALE"].includes(normalized)) return "warning";
  if (["ERROR", "FAILED", "FAILED_RESOURCE", "UNHEALTHY", "STOPPED"].includes(normalized)) return "error";
  if (["TECHNICAL_FAILURE", "CANCELLED"].includes(normalized)) return "error";
  return "unknown";
}

function StatusIcon({ tone, size = 16 }: { tone: HealthTone; size?: number }) {
  if (tone === "healthy") return <CheckCircle2 size={size} aria-hidden="true" />;
  if (tone === "running") return <CircleDotDashed size={size} aria-hidden="true" />;
  if (tone === "warning") return <TriangleAlert size={size} aria-hidden="true" />;
  if (tone === "error") return <XCircle size={size} aria-hidden="true" />;
  return <CircleAlert size={size} aria-hidden="true" />;
}

function StatusBadge({ status, label, outcome }: { status?: string | null; label?: string; outcome?: OutcomeStatus | null }) {
  const displayStatus = outcome ? statusFromOutcome(outcome) : status;
  const tone = toneForStatus(displayStatus);
  return (
    <span className={`status-badge status-${tone}`}>
      <StatusIcon tone={tone} size={14} />
      {label ?? statusLabel(displayStatus)}
    </span>
  );
}

function statusLabel(status?: string | null): string {
  const labels: Record<string, string> = {
    HEALTHY: "正常",
    OK: "正常",
    READY: "就绪",
    READY_TO_PUBLISH: "待发布",
    PUBLISHED: "已发布",
    NOOP: "无需执行",
    NOOP_NO_DIRTY: "无待更新数据",
    BLOCKED_SOURCE_GENERATION: "源代际阻断",
    FAILED_RESOURCE: "资源失败",
    COMPLETED: "已完成",
    VALIDATED: "已验证",
    VALIDATED_NO_OPPORTUNITY: "已验证·当前无机会",
    VALIDATED_NO_ACTION: "已验证·无需行动",
    VALIDATED_NO_SETUP: "已验证·无合格形态",
    VALIDATED_UNDERFILLED_MARKET: "已验证·市场机会较少",
    READY_DEGRADED: "就绪·降级",
    DEGRADED_UNDERFILLED_DATA_GAP: "降级·事实覆盖不足",
    NOT_RUN_UPSTREAM_BLOCKED: "未执行·上游阻断",
    BLOCKED_DATA_COVERAGE: "阻断·数据覆盖不足",
    BLOCKED_EVIDENCE_GAP: "阻断·事实证据不足",
    BLOCKED_MODEL: "阻断·模型失败",
    BLOCKED_TECHNICAL_DATA: "阻断·技术数据不足",
    ACTIVE: "活动",
    RUNNING: "运行中",
    STALE: "进度失联",
    IN_PROGRESS: "进行中",
    DEGRADED: "部分降级",
    BLOCKED: "已阻断",
    UPSTREAM_NOT_RUN: "上游未运行",
    DATA_INSUFFICIENT: "数据不足",
    TECHNICAL_FAILURE: "技术失败",
    CANCELLED: "已取消",
    FAILED: "执行失败",
    ERROR: "错误",
    STOPPED: "已停止",
    UNKNOWN: "未知",
    NOT_RUN: "尚未运行",
    QUEUED: "等待执行",
    EMPTY_SCOPE: "无有效范围",
    SUCCEEDED_NO_ACTION: "已完成·无动作",
    DATA_BLOCK: "数据阻断",
    EFFECTIVE_SIGNAL: "产生有效信号",
    PASS: "通过",
    DISABLED: "已禁用",
  };
  const key = (status ?? "UNKNOWN").toUpperCase();
  return labels[key] ?? codeLabel(status) ?? "未知";
}

function formatDateTime(value?: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

function formatDuration(milliseconds?: number | null): string {
  if (milliseconds === undefined || milliseconds === null || !Number.isFinite(milliseconds)) return "—";
  if (milliseconds < 1000) return `${Math.round(milliseconds)} 毫秒`;
  const seconds = Math.floor(milliseconds / 1000);
  if (seconds < 60) return `${seconds} 秒`;
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  return `${minutes} 分 ${rest} 秒`;
}

function formatMoney(value?: number | null): string {
  if (value === undefined || value === null || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("zh-CN", { style: "currency", currency: "CNY", maximumFractionDigits: 2 }).format(value);
}

function modelLabel(lane: LaneSummary): string {
  return MODEL_LABELS[lane.laneId] ?? modelNameLabel(lane.model ?? lane.laneId);
}

function nextScheduleLabel(overview: OverviewResponse): string {
  if (!overview.service.schedulerEnabled || !overview.schedule.some((item) => (item.status ?? "").toUpperCase() === "ACTIVE")) return "调度已禁用（验收模式）";
  const explicit = overview.schedule.filter((item) => item.nextRunAt).sort((left, right) => String(left.nextRunAt).localeCompare(String(right.nextRunAt)))[0];
  if (explicit) return `${formatDateTime(explicit.nextRunAt)} ${explicit.label}`;
  const parts = new Intl.DateTimeFormat("en-GB", { timeZone: "Asia/Shanghai", hour: "2-digit", minute: "2-digit", hour12: false }).formatToParts(new Date());
  const hour = Number(parts.find((part) => part.type === "hour")?.value ?? "0");
  const minute = Number(parts.find((part) => part.type === "minute")?.value ?? "0");
  const minuteOfDay = hour * 60 + minute;
  const premarket = overview.schedule.find((item) => item.id === "premarket");
  const morning = overview.schedule.find((item) => item.id === "morning");
  const close = overview.schedule.find((item) => item.id === "close");
  const a1 = overview.schedule.find((item) => item.id === "a1");
  if (minuteOfDay < 8 * 60 + 30 && premarket) return `08:30 ${premarket.label}`;
  if (minuteOfDay < 9 * 60 + 26 && morning) return `09:26 ${morning.label}`;
  if (minuteOfDay < 15 * 60 + 10 && close) return `15:10 ${close.label}`;
  if (minuteOfDay < 18 * 60 && a1) return `18:00 ${a1.label}（到点后由交易日历判定是否执行）`;
  return premarket ? `下一工作日 08:30 ${premarket.label}` : morning ? `下一工作日 09:26 ${morning.label}` : "等待调度信息";
}

function serviceHeadline(overview: OverviewResponse): { title: string; detail: string; tone: HealthTone } {
  if (overview.activeJob) {
    return { title: `${jobLabel(overview.activeJob.job)}正在运行`, detail: `开始于 ${formatDateTime(overview.activeJob.startedAt)}`, tone: "running" };
  }
  if (overview.service.schedulerEnabled === false) return { title: "服务在线，调度已禁用", detail: "当前为只读验收模式，不会触发研究或盯盘任务", tone: "warning" };
  const tone = toneForStatus(overview.service.status);
  if (tone === "healthy") return { title: "系统运行正常", detail: "控制台服务在线，工作流状态可读取", tone };
  if (tone === "warning") return { title: "系统需要关注", detail: "服务在线，但部分能力或最近运行处于降级状态", tone };
  if (tone === "error") return { title: "系统运行异常", detail: "请查看部署状态和最新错误日志", tone };
  return { title: "正在确认系统状态", detail: "尚未取得完整运行信息", tone: "unknown" };
}

function Panel({ title, icon, action, children, className = "" }: { title: string; icon?: ReactNode; action?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`panel ${className}`}>
      <header className="panel-header">
        <div className="panel-title">{icon}{title}</div>
        {action ? <div className="panel-action">{action}</div> : null}
      </header>
      {children}
    </section>
  );
}

function EmptyState({ title, detail, icon }: { title: string; detail: string; icon?: ReactNode }) {
  return (
    <div className="empty-state">
      <div className="empty-icon">{icon ?? <CircleAlert size={22} />}</div>
      <strong>{title}</strong>
      <span>{detail}</span>
    </div>
  );
}

function LoadingScreen() {
  return (
    <div className="loading-layout" aria-label="正在读取运行状态">
      <div className="skeleton skeleton-banner" />
      <div className="loading-columns">
        <div className="skeleton skeleton-large" />
        <div className="skeleton skeleton-large" />
      </div>
      <div className="skeleton skeleton-table" />
    </div>
  );
}

function AuthGate({ onAuthenticated }: { onAuthenticated: () => void }) {
  const [token, setToken] = useState("");

  function submit(event: FormEvent) {
    event.preventDefault();
    saveToken(token);
    onAuthenticated();
  }

  return (
    <main className="auth-page">
      <form className="auth-panel" onSubmit={submit}>
        <div className="brand-mark brand-mark-large"><Activity size={23} /></div>
        <p className="eyebrow">私人运行控制台</p>
        <h1>连接量见工作流</h1>
        <p>服务器已启用访问令牌。令牌只保存在当前浏览器会话中，不会写入日志或项目文件。</p>
        <label htmlFor="dashboard-token">访问令牌</label>
        <div className="token-field">
          <KeyRound size={17} aria-hidden="true" />
          <input id="dashboard-token" type="password" autoComplete="current-password" value={token} onChange={(event) => setToken(event.target.value)} required />
        </div>
        <button className="primary-button" type="submit">连接控制台</button>
      </form>
    </main>
  );
}

export function App() {
  const [view, setView] = useState<ViewId>("overview");
  const [overview, setOverview] = useState<OverviewResponse>(EMPTY_OVERVIEW);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [unauthorized, setUnauthorized] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [streamConnected, setStreamConnected] = useState(false);

  const load = useCallback(async (quiet = false) => {
    if (!quiet) setRefreshing(true);
    const controller = new AbortController();
    try {
      const [overviewData, runsData, logsData] = await Promise.all([
        apiFetch<OverviewResponse>("/api/overview", controller.signal),
        apiFetch<RunsResponse>(withQuery("/api/runs", { limit: 30 }), controller.signal),
        apiFetch<LogsResponse>(withQuery("/api/logs", { limit: 200 }), controller.signal),
      ]);
      const normalized = normalizeOverview(overviewData);
      setOverview(normalized);
      setRuns(Array.isArray(runsData.runs) ? runsData.runs : []);
      setLogs(Array.isArray(logsData.logs) ? logsData.logs : normalized.recentLogs);
      setUnauthorized(false);
      setError(null);
      setLastUpdated(new Date());
    } catch (caught) {
      if (caught instanceof ApiError && caught.status === 401) {
        setUnauthorized(true);
      } else {
        setError(caught instanceof Error ? caught.message : "无法读取运行状态");
      }
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
    return () => controller.abort();
  }, []);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(true), 10_000);
    return () => window.clearInterval(timer);
  }, [load]);

  useEffect(() => {
    if (unauthorized) return;
    let disposed = false;
    let reconnectTimer: number | undefined;
    let controller: AbortController | undefined;

    const connect = async (): Promise<void> => {
      controller = new AbortController();
      const token = getStoredToken();
      try {
        const response = await fetch("/api/logs/stream", {
          signal: controller.signal,
          headers: token ? { Authorization: `Bearer ${token}` } : undefined,
          cache: "no-store",
        });
        if (response.status === 401) {
          setUnauthorized(true);
          return;
        }
        if (!response.ok || !response.body) throw new Error(`HTTP ${response.status}`);
        setStreamConnected(true);
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (!disposed) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const frames = buffer.split(/\r?\n\r?\n/);
          buffer = frames.pop() ?? "";
          for (const frame of frames) {
            const data = frame.split(/\r?\n/).find((line) => line.startsWith("data: "))?.slice(6);
            if (!data) continue;
            try {
              const event = JSON.parse(data) as LogEntry;
              if (!event.timestamp || !event.message || !event.level) continue;
              setLogs((current) => {
                const key = `${event.id ?? ""}|${event.timestamp}|${event.job ?? ""}|${event.message}`;
                const withoutDuplicate = current.filter((item) => `${item.id ?? ""}|${item.timestamp}|${item.job ?? ""}|${item.message}` !== key);
                return [event, ...withoutDuplicate].sort((left, right) => right.timestamp.localeCompare(left.timestamp)).slice(0, 500);
              });
            } catch {
              // Ignore a malformed event and keep the stream alive.
            }
          }
        }
      } catch {
        if (!disposed) setStreamConnected(false);
      } finally {
        if (!disposed) reconnectTimer = window.setTimeout(() => void connect(), 3_000);
      }
    };

    void connect();
    return () => {
      disposed = true;
      setStreamConnected(false);
      controller?.abort();
      if (reconnectTimer !== undefined) window.clearTimeout(reconnectTimer);
    };
  }, [unauthorized]);

  const issues = useMemo(() => collectWorkbenchIssues(overview, logs), [overview, logs]);
  const openIssueCount = issues.filter((issue) => issue.status === "OPEN").length;

  if (unauthorized) {
    return <AuthGate onAuthenticated={() => void load()} />;
  }

  const currentNav = NAVIGATION.find((item) => item.id === view) ?? NAVIGATION[0];

  return (
    <div className="app-shell">
      <aside className={`sidebar ${sidebarOpen ? "sidebar-open" : ""}`}>
        <div className="brand">
          <div className="brand-mark"><Activity size={19} aria-hidden="true" /></div>
          <div><strong>量见</strong><span>运行控制台</span></div>
          <button className="icon-button sidebar-close" type="button" onClick={() => setSidebarOpen(false)} aria-label="关闭导航"><X size={20} /></button>
        </div>
        <nav aria-label="主要导航">
          {NAVIGATION.map((item) => {
            const Icon = item.icon;
            return (
              <button key={item.id} type="button" className={item.id === view ? "nav-item nav-item-active" : "nav-item"} onClick={() => { setView(item.id); setSidebarOpen(false); }}>
                <Icon size={18} aria-hidden="true" />
                <span>{item.label}</span>
                {item.id === "issues" && openIssueCount > 0 ? <span className="nav-count" aria-label={`${openIssueCount} 个待处理问题`}>{openIssueCount}</span> : null}
              </button>
            );
          })}
        </nav>
        <div className="sidebar-foot">
          <ShieldCheck size={17} aria-hidden="true" />
          <div><strong>仅内部模拟</strong><span>不连接真实账户</span></div>
        </div>
      </aside>

      <div className="app-main">
        <header className="topbar">
          <button className="icon-button mobile-menu" type="button" onClick={() => setSidebarOpen(true)} aria-label="打开导航"><Menu size={21} /></button>
          <div className="topbar-title"><span>{currentNav?.label}</span><small>北京时间</small></div>
          <div className="topbar-meta">
            <span className={`connection-dot status-${toneForStatus(overview.service.status)}`} aria-hidden="true" />
            <span>{statusLabel(overview.service.status)}</span>
            <span className="topbar-divider" />
            <time>{lastUpdated ? `${formatDateTime(lastUpdated.toISOString())} 更新` : "等待更新"}</time>
            <button className="icon-button" type="button" onClick={() => void load()} disabled={refreshing} aria-label="刷新运行状态">
              <RefreshCw size={17} className={refreshing ? "spin" : ""} />
            </button>
          </div>
        </header>

        <main className="content" id="main-content">
          {error ? (
            <div className="error-banner" role="alert"><TriangleAlert size={18} /><span><strong>状态读取失败。</strong> {error}，控制台会继续自动重试。</span></div>
          ) : null}
          {loading ? <LoadingScreen /> : (
            <>
              {view === "overview" ? <OverviewPage overview={overview} logs={logs} issues={issues} onNavigate={setView} /> : null}
              {view === "funnel" ? <FunnelPage overview={overview} runs={runs} /> : null}
              {view === "monitor" ? <MonitorPage overview={overview} /> : null}
              {view === "review" ? <A5ReviewPage reviews={overview.recentA5Reviews ?? []} /> : null}
              {view === "accounts" ? <AccountsPage overview={overview} /> : null}
              {view === "issues" ? <IssuesPage issues={issues} /> : null}
              {view === "logs" ? <LogsPage logs={logs} streamConnected={streamConnected} /> : null}
              {view === "deployment" ? <DeploymentPage overview={overview} /> : null}
            </>
          )}
        </main>
      </div>
      {sidebarOpen ? <button className="sidebar-scrim" type="button" aria-label="关闭导航" onClick={() => setSidebarOpen(false)} /> : null}
    </div>
  );
}

function OverviewPage({ overview, logs, issues, onNavigate }: { overview: OverviewResponse; logs: LogEntry[]; issues: WorkbenchIssue[]; onNavigate: (view: ViewId) => void }) {
  const headline = serviceHeadline(overview);
  return (
    <div className="page-stack">
      <section className={`status-hero hero-${headline.tone}`}>
        <div className="status-hero-icon"><StatusIcon tone={headline.tone} size={27} /></div>
        <div className="status-hero-copy"><h1>{headline.title}</h1><p>{headline.detail}</p></div>
        <div className="status-hero-next"><CalendarClock size={20} /><div><span>下一次计划</span><strong>{nextScheduleLabel(overview)}</strong></div></div>
      </section>

      <div className="overview-grid">
        <FunnelPanel workflow={overview.latestWorkflow} onOpen={() => onNavigate("funnel")} />
        <div className="overview-rail">
          <MonitorPanel overview={overview} onOpen={() => onNavigate("monitor")} />
          <DataSourcesPanel sources={overview.dataSources} onOpen={() => onNavigate("deployment")} />
        </div>
      </div>

      <WorkflowProgressPanel progress={overview.workflowProgress} />

      <IssuePanel issues={issues} onOpen={() => onNavigate("issues")} />

      <LogPanel logs={logs.slice(0, 10)} onOpen={() => onNavigate("logs")} />
    </div>
  );
}

interface StageDetailTarget {
  runId: string;
  laneId: string;
  model: string;
  modelLabel: string;
  stage: StageSummary;
  downstreamInputCount?: number | null;
}

function FunnelPanel({ workflow, onOpen }: { workflow: OverviewResponse["latestWorkflow"]; onOpen: () => void }) {
  const stages = ["A1", "A2", "A3"];
  const [detailTarget, setDetailTarget] = useState<StageDetailTarget | null>(null);
  const returnFocusRef = useRef<HTMLButtonElement | null>(null);

  function openStageDetail(target: StageDetailTarget, trigger: HTMLButtonElement): void {
    returnFocusRef.current = trigger;
    setDetailTarget(target);
  }

  function dismissStageDetail(): void {
    setDetailTarget(null);
    window.requestAnimationFrame(() => returnFocusRef.current?.focus());
  }

  return (
    <>
      <Panel title="研究漏斗" icon={<GitBranch size={18} />} action={<button className="text-button" type="button" onClick={onOpen}>查看运行详情</button>} className="funnel-panel">
        <div className="workflow-meta">
          <div><span>最近运行</span><strong>{workflow.runId ? "最近一次研究" : "尚未运行"}</strong></div>
          <div><span>时段</span><strong>{slotLabel(workflow.slot)}</strong></div>
          <div><span>状态</span><StatusBadge status={workflow.status} outcome={workflow.outcome} /></div>
          <div><span>更新时间</span><strong>{formatDateTime(workflow.updatedAt)}</strong></div>
        </div>
        {workflow.lanes.length === 0 ? (
          <EmptyState title="暂无模型运行记录" detail="收盘研究运行后，三个模型的 A1–A3 阶段会在这里逐项显示。" icon={<GitBranch size={22} />} />
        ) : (
          <div className="funnel-table-wrap">
            <table className="funnel-table">
              <thead><tr><th>模型</th>{stages.map((stage) => <th key={stage}>{STAGE_LABELS[stage]}</th>)}</tr></thead>
              <tbody>{workflow.lanes.map((lane) => (
                <tr key={lane.laneId}>
                  <th scope="row"><span className="model-name">{modelLabel(lane)}</span><small>独立研究模型</small></th>
                  {stages.map((stage) => {
                    const summary = lane.stages.find((item) => item.stage.toUpperCase() === stage);
                    const downstreamInputCount = stage === "A2"
                      ? lane.stages.find((item) => item.stage.toUpperCase() === "A3")?.outcome?.counts.input ?? null
                      : null;
                    return <StageCell key={stage} stage={summary} model={modelLabel(lane)} downstreamInputCount={downstreamInputCount} canOpen={Boolean(workflow.runId && summary)} onOpen={(trigger) => {
                      if (!workflow.runId || !summary) return;
                      openStageDetail({ runId: workflow.runId, laneId: lane.laneId, model: lane.model, modelLabel: modelLabel(lane), stage: summary, downstreamInputCount }, trigger);
                    }} />;
                  })}
                </tr>
              ))}</tbody>
            </table>
          </div>
        )}
        <div className="panel-footnote"><Database size={14} /><span>点击任一阶段可查看该模型本次筛选的股票、原因、证据与风险；无数据时不会估算。</span></div>
      </Panel>
      <StageDetailDialog target={detailTarget} onDismiss={dismissStageDetail} />
    </>
  );
}

function progressCount(value: number | null | undefined): string {
  return value === null || value === undefined || !Number.isFinite(value) ? "—" : new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 0 }).format(value);
}

function progressPair(processed: number | null | undefined, total: number | null | undefined): string {
  return `${progressCount(processed)} / ${progressCount(total)}`;
}

function progressPhaseLabel(phase?: string | null): string {
  const labels: Record<string, string> = {
    STARTING: "启动准备",
    UNIVERSE_SYNC: "全市场股票池",
    MARKET_FACT_SYNC: "行情事实同步",
    CNINFO_SYNC: "巨潮公告同步",
    CNINFO_PDF_SYNC: "巨潮公告原文证据提取",
    FACT_MANIFEST_SYNC: "事实清单落盘",
    OPEN_MACRO_SYNC: "宏观与大类资产数据",
    DATA_SYNC: "数据同步",
    SNAPSHOT: "冻结快照",
    FEATURE_SOURCE_GENERATION: "特征源代际物化",
    FEATURE_MAINTENANCE_PRECHECK: "特征维护预检",
    FEATURE_MAINTENANCE_SOURCE_SELECT: "选择特征源代际",
    FEATURE_MAINTENANCE_BUILD: "特征代际构建",
    FEATURE_MAINTENANCE_VALIDATE: "特征代际校验",
    FEATURE_MAINTENANCE_PUBLISH: "特征代际发布",
    FEATURE_MAINTENANCE_NOOP: "特征维护无需执行",
    SNAPSHOT_RESUMED: "恢复已冻结快照",
    MACRO_DISCOVERY: "宏观与产业链发现",
    RESEARCH_MACRO_DISCOVERY: "宏观与产业链发现",
    A1_LOCAL_SCREEN: "A1 本地确定性筛选",
    RESEARCH_A1_LOCAL_SCREEN: "A1 本地确定性筛选",
    A1_LLM_REVIEW: "A1 模型复核",
    RESEARCH_A1_LLM_REVIEW: "A1 模型复核",
    A2_LOCAL_ROLE: "A2 本地角色识别",
    RESEARCH_A2_LOCAL_ROLE: "A2 本地角色识别",
    A2_LLM_REVIEW: "A2 模型复核",
    RESEARCH_A2_LLM_REVIEW: "A2 模型复核",
    A3_LOCAL_TECHNICAL: "A3 本地技术门禁",
    RESEARCH_A3_LOCAL_TECHNICAL: "A3 本地技术门禁",
    A3_LLM_REVIEW: "A3 模型计划复核",
    RESEARCH_A3_LLM_REVIEW: "A3 模型计划复核",
    A1: "A1 基本面",
    A2: "A2 主题情绪",
    A3: "A3 技术计划",
    RESEARCH_A1: "A1 基本面研究",
    RESEARCH_A2: "A2 主题情绪研究",
    RESEARCH_A3: "A3 技术计划研究",
    RESEARCH: "模型研究",
    PERSIST: "结果落盘",
    COMPLETE: "已完成",
    COMPLETED: "已完成",
    DATA_READY: "数据已就绪",
    DATA_PARTIAL: "数据部分就绪",
    FAILED: "执行失败",
    DONE: "已完成",
    READY: "已就绪",
    BOOTSTRAP: "首次初始化",
    UNKNOWN: "未知阶段",
  };
  return labels[phase ?? "UNKNOWN"] ?? "未知阶段";
}

function progressIssueTitle(issue: WorkflowProgressSummary["issue"]): string {
  if (issue === "UNREADABLE") return "进度文件不可读";
  if (issue === "OVERSIZE") return "进度读取已阻断";
  if (issue === "INVALID_JSON") return "进度文件格式无效";
  if (issue === "INVALID_SHAPE") return "进度文件结构无效";
  if (issue === "HEARTBEAT_TIMEOUT") return "进度失联";
  return "进度不可用";
}

function progressIssueLabel(issue?: WorkflowProgressSummary["issue"]): string {
  if (issue === "UNREADABLE") return "无法读取进度文件，控制台会继续自动重试";
  if (issue === "OVERSIZE") return "进度文件超过安全大小限制，已阻断读取";
  if (issue === "INVALID_JSON") return "进度文件格式无效，已停止展示原文";
  if (issue === "INVALID_SHAPE") return "进度文件结构无效，已停止展示原文";
  if (issue === "HEARTBEAT_TIMEOUT") return "超过阈值未更新，任务可能退出或卡住，请查看日志";
  return "";
}

function progressReasonLabel(reason?: string | null): string | null {
  if (!reason) return null;
  const labels: Record<string, string> = {
    NOOP_NO_DIRTY: "无待更新数据，未读取历史大快照",
    NON_MAINTENANCE_DAY: "非维护日，无需执行",
    FEATURE_MAINTENANCE_DISABLED: "特征维护已独立禁用",
    FEATURE_MAINTENANCE_BUSY: "已有特征维护进程运行，本次未重复启动",
    FEATURE_SOURCE_GENERATION_MISSING: "缺少可用的特征源代际",
    FEATURE_SOURCE_GENERATION_AMBIGUOUS: "最新特征源代际存在歧义",
    FEATURE_SOURCE_MARKET_DATA_STALE: "特征源行情日期未达到要求",
    LIVE_SOURCE_NOT_AVAILABLE: "缺少已封存且验证通过的特征源代际",
    LIVE_SOURCE_AMBIGUOUS: "同一时点存在相互冲突的特征源代际",
    LIVE_SOURCE_INCOMPATIBLE: "特征源版本尚未覆盖本次增量更新",
    FEATURE_ACTIVE_GENERATION_MISSING: "缺少可供增量更新的活动特征代际",
    FEATURE_FULL_REBUILD_STORAGE_WATERMARK_BLOCKED: "磁盘水位不足，已阻止全量特征重建",
    FEATURE_INCREMENTAL_STORAGE_WATERMARK_BLOCKED: "磁盘水位不足，已阻止增量特征更新",
    FAILED_RESOURCE: "进程因资源限制失败",
    PUBLISHED: "新特征代际已校验并发布",
  };
  return labels[reason] ?? codeLabel(reason);
}

type ProgressMeasure = {
  processed: number;
  total: number;
  kind: "stocks" | "batches";
};

function hasMeaningfulProgress(processed: number | null, total: number | null): processed is number {
  return processed !== null && Number.isFinite(processed) && total !== null && Number.isFinite(total) && total > 0;
}

function progressMeasure(
  processed: number | null,
  total: number | null,
  batchProcessed: number | null,
  batchTotal: number | null,
): ProgressMeasure | null {
  if (hasMeaningfulProgress(processed, total)) return { processed, total: total as number, kind: "stocks" };
  if (hasMeaningfulProgress(batchProcessed, batchTotal)) return { processed: batchProcessed, total: batchTotal as number, kind: "batches" };
  return null;
}

function isResearchPhase(phase?: string | null): boolean {
  return phase?.startsWith("RESEARCH_") === true;
}

function isMacroDiscoveryPhase(phase?: string | null): boolean {
  return phase?.includes("MACRO_DISCOVERY") === true;
}

function discoveryMetricLabel(value: number | null, suffix: string): string {
  return value === null ? `${suffix}待返回` : `${value} ${suffix}`;
}

function progressDiagnosticLabel(diagnostics: WorkflowProgressStage["diagnostics"] | null | undefined): string | null {
  if (!diagnostics) return null;
  const details: string[] = [];
  const shape = diagnostics.lastInvalidOutputShape;
  if (shape) {
    const shapeDetails = [`返回结构 ${shape.type ?? "未知"}`, `已识别字段 ${shape.fields.length} 个`];
    if (shape.unknownFieldCount !== null && shape.unknownFieldCount > 0) shapeDetails.push(`未识别字段 ${progressCount(shape.unknownFieldCount)} 个`);
    if (shape.envelopeUnknownFieldCount !== null && shape.envelopeUnknownFieldCount > 0) shapeDetails.push(`外层未识别字段 ${progressCount(shape.envelopeUnknownFieldCount)} 个`);
    details.push(shapeDetails.join(" · "));
  }
  if (diagnostics.semanticAttempts !== null) details.push(`语义尝试 ${progressCount(diagnostics.semanticAttempts)} 次`);
  if (diagnostics.themeCount !== null) details.push(`主题 ${progressCount(diagnostics.themeCount)}`);
  if (diagnostics.nodeCount !== null) details.push(`节点 ${progressCount(diagnostics.nodeCount)}`);
  if (diagnostics.mappingCount !== null && diagnostics.expectedMappingCount !== null) {
    details.push(`映射 ${progressCount(diagnostics.mappingCount)} / ${progressCount(diagnostics.expectedMappingCount)}`);
  } else if (diagnostics.mappingCount !== null) {
    details.push(`映射 ${progressCount(diagnostics.mappingCount)}`);
  } else if (diagnostics.expectedMappingCount !== null) {
    details.push(`应有映射 ${progressCount(diagnostics.expectedMappingCount)}`);
  }
  if (diagnostics.missingMappingCount !== null) details.push(`缺失映射 ${progressCount(diagnostics.missingMappingCount)}`);
  return details.length ? `安全诊断：${details.join("；")}` : null;
}

function progressPercent(processed: number | null, total: number | null): number | null {
  if (processed === null || total === null || total <= 0) return null;
  return Math.max(0, Math.min(100, (processed / total) * 100));
}

function ProgressBar({ processed, total, compact = false }: { processed: number | null; total: number | null; compact?: boolean }) {
  const percent = progressPercent(processed, total);
  if (percent === null) return null;
  return <div className={`progress-bar ${compact ? "progress-bar-compact" : ""}`} role="progressbar" aria-valuemin={0} aria-valuemax={total ?? undefined} aria-valuenow={processed ?? undefined} aria-label="执行进度"><span style={{ transform: `scaleX(${percent / 100})` }} /></div>;
}

function progressRate(progress: WorkflowProgressSummary): string {
  if (!progress.phaseStartedAt || !progress.processed || progress.processed < 1) return "—";
  const startedAt = Date.parse(progress.phaseStartedAt);
  const updatedAt = Date.parse(progress.updatedAt ?? "");
  if (!Number.isFinite(startedAt) || !Number.isFinite(updatedAt) || updatedAt <= startedAt) return "—";
  const perMinute = progress.processed / ((updatedAt - startedAt) / 60_000);
  if (!Number.isFinite(perMinute) || perMinute <= 0) return "—";
  return `${perMinute < 10 ? perMinute.toFixed(1) : Math.round(perMinute)} 份/分钟`;
}

function WorkflowProgressPanel({ progress }: { progress: WorkflowProgressSummary | null }) {
  const isPdfProgress = progress?.phase === "CNINFO_PDF_SYNC";
  const isMarketFactProgress = progress?.phase === "MARKET_FACT_SYNC";
  const hasBlockingIssue = Boolean(progress?.issue && !progress.stale);
  const overallMeasure = progress ? progressMeasure(progress.processed, progress.total, null, null) : null;
  const researchWithoutOverallCount = progress ? isResearchPhase(progress.phase) && !overallMeasure : false;
  const resourceSummary = progress?.resources
    ? [
      progress.resources.rssCurrentMb !== null ? `当前内存 ${Math.round(progress.resources.rssCurrentMb)} 兆字节` : null,
      progress.resources.rssPeakMb !== null ? `内存峰值 ${Math.round(progress.resources.rssPeakMb)} 兆字节` : null,
      progress.resources.systemMemAvailableMb !== null ? `可用内存 ${Math.round(progress.resources.systemMemAvailableMb)} 兆字节` : null,
      progress.resources.diskFreeMb !== null ? `磁盘可用 ${Math.round(progress.resources.diskFreeMb / 1024)} 吉字节` : null,
    ].filter(Boolean).join(" · ")
    : "";
  return (
    <Panel title="执行进度" icon={<Activity size={18} />} className="workflow-progress-panel">
      {!progress ? <EmptyState title="暂无持久化进度" detail="首次初始化或研究任务开始后，研究引擎会将阶段进度写入控制台。" icon={<Activity size={22} />} /> : hasBlockingIssue ? (
        <div className="progress-issue" role="status"><StatusIcon tone={progress.status === "BLOCKED" ? "warning" : "error"} size={20} /><div><strong>{progressIssueTitle(progress.issue)}</strong><span>{progressIssueLabel(progress.issue)}</span></div></div>
      ) : (
        <>
          {progress.stale ? <div className="progress-stale" role="status"><StatusIcon tone="warning" size={17} /><div><strong>{progress.staleIssue === "HEARTBEAT_TIMEOUT" ? "进度失联" : "更新暂时延迟"}</strong><span>{progressIssueLabel(progress.staleIssue ?? "UNREADABLE")}</span></div></div> : null}
          <div className="progress-summary-grid">
            <div><span>当前阶段</span><strong>{progressPhaseLabel(progress.phase)}</strong><StatusBadge status={progress.status} /></div>
            <div><span>{isPdfProgress ? "公告原文处理" : researchWithoutOverallCount ? "研究批次（按模型）" : "总体处理"}</span><strong>{overallMeasure ? progressPair(overallMeasure.processed, overallMeasure.total) : researchWithoutOverallCount ? "按下方模型批次" : "暂无可用计数"}</strong>{overallMeasure ? <ProgressBar processed={overallMeasure.processed} total={overallMeasure.total} /> : <span className="progress-no-value">{researchWithoutOverallCount ? "各模型分别统计" : "暂未提供"}</span>}</div>
            <div><span>{isPdfProgress ? "公告原文缓存命中 / 未命中" : isMarketFactProgress ? "纯缓存 / 有增量" : "缓存命中 / 未命中"}</span><strong>{progressPair(progress.cacheHits, progress.cacheMisses)}</strong></div>
            <div><span>{isPdfProgress ? "公告原文失败数" : "失败数"}</span><strong className={progress.failures ? "progress-danger" : ""}>{progressCount(progress.failures)}</strong></div>
            <div><span>已用时间</span><strong>{formatDuration(progress.elapsedMs)}</strong></div>
            <div><span>预计剩余</span><strong>{formatDuration(progress.etaMs)}</strong></div>
          </div>
          {isPdfProgress ? (
            <div className="progress-current-task" aria-live="polite">
              <div><span>最近完成股票</span><strong>{progress.currentSymbol ? stockSymbolLabel(progress.currentSymbol) : "等待首份完成"}</strong></div>
              <div><span>最近完成文档</span><strong>{progress.currentDocument ? "最新文档已处理" : "正在生成并行任务"}</strong></div>
              <div><span>成功 / 失败</span><strong>{progressPair(progress.documentsSucceeded, progress.documentsFailed)}</strong></div>
              <div><span>处理速度</span><strong>{progressRate(progress)}</strong></div>
            </div>
          ) : null}
          {isMarketFactProgress ? (
            <div className="progress-current-task" aria-live="polite">
              <div><span>最近处理股票</span><strong>{progress.currentSymbol ? stockSymbolLabel(progress.currentSymbol) : "等待首支完成"}</strong></div>
              <div><span>日线尾部增量</span><strong>{progressCount(progress.dailyUpdates)}</strong></div>
              <div><span>财务轮换刷新</span><strong>{progressCount(progress.financialRefreshes)}</strong></div>
              <div><span>延期财务刷新</span><strong>{progressCount(progress.deferredFinancialRefreshes)}</strong></div>
            </div>
          ) : null}
          {progress.lanes.length === 0 ? <div className="progress-empty-lanes">当前阶段尚未产生模型批次。</div> : <div className="progress-lanes">{progress.lanes.map((lane) => <ProgressLane key={lane.laneId} lane={lane} />)}</div>}
          <div className="panel-footnote"><Activity size={14} /><span>{progress.stale
            ? progress.staleIssue === "HEARTBEAT_TIMEOUT"
              ? `最后更新时间 ${formatDateTime(progress.updatedAt)}；超过阈值未更新，任务可能退出或卡住，请查看日志。`
              : `更新暂时延迟，正在重试；以下为最近一次成功读取的安全汇总（${formatDateTime(progress.updatedAt)}）。`
            : `最近更新时间 ${formatDateTime(progress.updatedAt)}；此处只显示安全的汇总进度，不展示模型原文。`}{progressReasonLabel(progress.reasonCode) ? ` 结果：${progressReasonLabel(progress.reasonCode)}。` : ""}{resourceSummary ? ` 资源：${resourceSummary}。` : ""}</span></div>
        </>
      )}
    </Panel>
  );
}

function ProgressLane({ lane }: { lane: WorkflowProgressLane }) {
  const macroDiscovery = isMacroDiscoveryPhase(lane.currentStage);
  const stockMeasure = hasMeaningfulProgress(lane.processed, lane.total);
  const batchMeasure = hasMeaningfulProgress(lane.batchProcessed, lane.batchTotal);
  const displayMeasure = progressMeasure(lane.processed, lane.total, lane.batchProcessed, lane.batchTotal);
  const discoveryPrimary = `行业 ${progressCount(lane.industryCount)} · 月度决策 ${progressCount(lane.monthlyDecisionCount)}`;
  const discoverySecondary = [
    discoveryMetricLabel(lane.themeCount, "主题"),
    discoveryMetricLabel(lane.nodeCount, "节点"),
    discoveryMetricLabel(lane.mappingCount, "映射"),
  ].join(" · ");
  return <article className="progress-lane"><header><div><strong>{MODEL_LABELS[lane.laneId] ?? modelNameLabel(lane.model)}</strong><small>独立研究模型</small></div><StatusBadge status={lane.status} label={lane.currentStage ? progressPhaseLabel(lane.currentStage) : statusLabel(lane.status)} /></header>
    <div className="progress-lane-meta"><span>{macroDiscovery ? discoveryPrimary : stockMeasure ? `股票 ${progressPair(lane.processed, lane.total)}` : "等待股票阶段"}</span><span>{macroDiscovery ? discoverySecondary : batchMeasure ? `批次 ${progressPair(lane.batchProcessed, lane.batchTotal)}` : "等待批次信息"}</span></div>
    {displayMeasure ? <ProgressBar processed={displayMeasure.processed} total={displayMeasure.total} compact /> : <span className="progress-no-value">暂无可用进度</span>}
    {lane.stages.length ? <ul className="progress-stage-list">{lane.stages.map((stage) => <ProgressStage key={stage.stage} stage={stage} />)}</ul> : null}
  </article>;
}

function ProgressStage({ stage }: { stage: WorkflowProgressStage }) {
  const macroDiscovery = isMacroDiscoveryPhase(stage.stage);
  const stockMeasure = hasMeaningfulProgress(stage.processed, stage.total);
  const batchMeasure = hasMeaningfulProgress(stage.batchProcessed, stage.batchTotal);
  const displayMeasure = progressMeasure(stage.processed, stage.total, stage.batchProcessed, stage.batchTotal);
  const metricLabel = macroDiscovery
    ? `行业 ${progressCount(stage.industryCount)} · 月度决策 ${progressCount(stage.monthlyDecisionCount)} · ${discoveryMetricLabel(stage.themeCount, "主题")} · ${discoveryMetricLabel(stage.nodeCount, "节点")} · ${discoveryMetricLabel(stage.mappingCount, "映射")}`
    : stockMeasure
    ? `股票 ${progressPair(stage.processed, stage.total)}${batchMeasure ? ` · 批次 ${progressPair(stage.batchProcessed, stage.batchTotal)}` : " · 批次数据未提供"}`
    : batchMeasure
      ? `股票计数未提供 · 批次 ${progressPair(stage.batchProcessed, stage.batchTotal)}`
      : "股票计数未提供 · 批次数据未提供";
  const isA2Stage = stage.stage.toUpperCase().includes("A2");
  const effectiveA2Count = isA2Stage && stage.selected !== null && stage.monitor !== null
    ? stage.selected + stage.monitor
    : null;
  const funnelCounts = isA2Stage
    ? [
        effectiveA2Count !== null ? `有效研究池 ${progressCount(effectiveA2Count)}` : null,
        stage.selected !== null ? `核心聚焦 ${progressCount(stage.selected)}` : null,
        stage.monitor !== null ? `扩展观察 ${progressCount(stage.monitor)}` : null,
        stage.rejected !== null ? `硬性淘汰 ${progressCount(stage.rejected)}` : null,
      ].filter(Boolean).join(" · ")
    : [
        stage.selected !== null ? `送模型 ${progressCount(stage.selected)}` : null,
        stage.monitor !== null ? `观察 ${progressCount(stage.monitor)}` : null,
        stage.rejected !== null ? `淘汰 ${progressCount(stage.rejected)}` : null,
      ].filter(Boolean).join(" · ");
  const diagnosticLabel = progressDiagnosticLabel(stage.diagnostics);
  return <li><div><strong>{progressPhaseLabel(stage.stage)}</strong><StatusBadge status={stage.status} /></div><span>{metricLabel}</span>{funnelCounts ? <span>{funnelCounts}</span> : null}{diagnosticLabel ? <span className="progress-diagnostics">{diagnosticLabel}</span> : null}{displayMeasure ? <ProgressBar processed={displayMeasure.processed} total={displayMeasure.total} compact /> : <span className="progress-no-value">暂无可用进度</span>}</li>;
}

function StageCell({ stage, model, downstreamInputCount, canOpen, onOpen }: { stage?: StageSummary; model: string; downstreamInputCount?: number | null; canOpen: boolean; onOpen: (trigger: HTMLButtonElement) => void }) {
  if (!stage) return <td><div className="stage-cell stage-unknown"><StatusBadge status="UNKNOWN" label="无记录" /><small>—</small></div></td>;
  const stageName = stage.stage.toUpperCase();
  const selectedCount = stage.symbolCount ?? stage.outcome?.counts.selected ?? null;
  const effectiveCount = stageName === "A2"
    ? stage.poolCounts?.effectiveResearch ?? stage.outcome?.counts.effective_research_pool ?? selectedCount
    : selectedCount;
  const countLabel = effectiveCount !== null && effectiveCount !== undefined
    ? stageName === "A2" ? `有效研究 ${progressCount(effectiveCount)} 只` : `${progressCount(effectiveCount)} 只`
    : "数量未知";
  const breakdown = stageName === "A2" && stage.poolCounts
    ? [
        stage.poolCounts.rotationDirections !== null && stage.poolCounts.rotationDirections !== undefined
          ? `${stage.poolCounts.rotationDirections} 个方向`
          : null,
        `聚焦 ${stage.poolCounts.approved}`,
        `观察 ${stage.poolCounts.watch}`,
        stage.poolCounts.a3Candidates !== null && stage.poolCounts.a3Candidates !== undefined
          ? `进入 A3 ${stage.poolCounts.a3Candidates}`
          : downstreamInputCount !== null && downstreamInputCount !== undefined
            ? `进入 A3 ${downstreamInputCount}`
          : null,
      ].filter(Boolean).join(" · ")
    : null;
  return (
    <td className="stage-cell-table-cell">
      <button className="stage-cell-trigger" type="button" disabled={!canOpen} onClick={(event) => onOpen(event.currentTarget)} aria-label={`查看 ${model} ${STAGE_LABELS[stage.stage.toUpperCase()] ?? stage.stage} 详情，${countLabel}`}>
        <StatusBadge status={stage.status} outcome={stage.outcome} />
        <span className="stage-count">{countLabel}</span>
        <small>{stage.latencyMs ? formatDuration(stage.latencyMs) : "耗时未知"}</small>
        {breakdown ? <span className="stage-count-breakdown">{breakdown}</span> : null}
      </button>
    </td>
  );
}

function fallbackPools(stage: string): StageDetailPool[] {
  const labels: Record<string, [string, string, string]> = {
    A1: ["晋级研究", "持续观察", "淘汰"],
    A2: ["聚焦候选", "仅观察", "淘汰"],
    A3: ["核心计划", "次级观察", "淘汰"],
  };
  const stageLabels = labels[stage.toUpperCase()] ?? ["晋级", "观察", "淘汰"];
  return (["approved", "watch", "rejected"] as StagePoolId[]).map((id, index) => ({ id, label: stageLabels[index], count: 0 }));
}

function stageDetailPath(target: StageDetailTarget, pool: StagePoolId, page: number, query: string, reason: string): string {
  const base = `/api/research/runs/${encodeURIComponent(target.runId)}/lanes/${encodeURIComponent(target.laneId)}/stages/${encodeURIComponent(target.stage.stage.toUpperCase())}`;
  return withQuery(base, { pool, page, pageSize: 50, q: query, reason });
}

function outcomeAxisLabel(outcome: OutcomeStatus): string {
  const quality = outcome.quality_state === "VALIDATED"
    ? "事实已验证"
    : outcome.quality_state === "DEGRADED"
      ? "证据降级"
      : outcome.quality_state === "BLOCKED"
        ? "已阻断"
        : outcome.quality_state === "FAILED" ? "技术失败" : "已取消";
  const stage = "stage" in outcome ? outcome.stage.toUpperCase() : null;
  const opportunityLabel = (value: string): string => value === "PRESENT" ? "存在机会" : value === "ABSENT" ? "已验证无机会" : value === "UNKNOWN" ? "机会未知" : "不适用";
  const actionabilityLabel = (value: string): string => value === "ACTIONABLE" ? "可行动" : value === "NO_ACTION" ? "无需行动" : value === "UNKNOWN" ? "行动未知" : "不适用";
  const opportunity = stage === "A1"
    ? `研究${opportunityLabel(outcome.research_opportunity_state)}`
    : stage === "A2"
      ? `聚焦${opportunityLabel(outcome.focus_opportunity_state)}`
      : stage === "A3"
        ? `行动${actionabilityLabel(outcome.actionability_state)}`
        : `研究${opportunityLabel(outcome.research_opportunity_state)} · 聚焦${opportunityLabel(outcome.focus_opportunity_state)} · 行动${actionabilityLabel(outcome.actionability_state)}`;
  const publication = outcome.publication_state === "PUBLISHED"
    ? "已发布"
    : outcome.publication_state === "READY" ? "可发布" : outcome.publication_state === "BLOCKED" ? "不可发布" : "不适用";
  return `${quality} · ${opportunity} · ${publication}`;
}

function OutcomeNotice({ outcome }: { outcome: OutcomeStatus | null | undefined }) {
  if (!outcome) return null;
  const reasons = outcome.reason_codes.length ? `；说明：${outcome.reason_codes.map((reason) => codeLabel(reason)).join("、")}` : "";
  const tone = outcome.quality_state === "FAILED" || outcome.quality_state === "CANCELLED" ? "error"
    : outcome.quality_state === "BLOCKED" || outcome.quality_state === "DEGRADED" || outcome.opportunity_state === "UNKNOWN" ? "warning"
      : "healthy";
  return <div className={`stage-detail-outcome outcome-${tone}`} role="status"><StatusIcon tone={tone} size={15} /><strong>{outcomeAxisLabel(outcome)}</strong><span>{`运行阶段：${statusLabel(outcome.lifecycle_state)}${reasons}`}</span></div>;
}

function StageDetailDialog({ target, onDismiss }: { target: StageDetailTarget | null; onDismiss: () => void }) {
  const dialogRef = useRef<HTMLDialogElement | null>(null);
  const [pool, setPool] = useState<StagePoolId>("approved");
  const [page, setPage] = useState(1);
  const [queryInput, setQueryInput] = useState("");
  const [query, setQuery] = useState("");
  const [reason, setReason] = useState("");
  const [data, setData] = useState<StageDetailResponse | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [mobileDetailOpen, setMobileDetailOpen] = useState(false);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    const handleClose = (): void => onDismiss();
    dialog.addEventListener("close", handleClose);
    return () => dialog.removeEventListener("close", handleClose);
  }, [onDismiss]);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    if (target && !dialog.open) dialog.showModal();
    if (!target && dialog.open) dialog.close();
  }, [target]);

  useEffect(() => {
    setPool("approved");
    setPage(1);
    setQueryInput("");
    setQuery("");
    setReason("");
    setData(null);
    setSelectedSymbol(null);
    setError(null);
    setMobileDetailOpen(false);
  }, [target?.runId, target?.laneId, target?.stage.stage]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setPage(1);
      setQuery(queryInput.trim());
    }, 250);
    return () => window.clearTimeout(timer);
  }, [queryInput]);

  useEffect(() => {
    if (!target) return;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    void apiFetch<StageDetailResponse>(stageDetailPath(target, pool, page, query, reason), controller.signal)
      .then((response) => {
        const normalized = { ...response, outcome: readStageOutcome(response.outcome ?? response) };
        setData(normalized);
        setSelectedSymbol((current) => normalized.items.some((item) => item.symbol === current) ? current : normalized.items[0]?.symbol ?? null);
      })
      .catch((caught) => {
        if (controller.signal.aborted) return;
        setError(caught instanceof Error ? caught.message : "无法读取阶段详情");
        setData(null);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [target, pool, page, query, reason]);

  const pools = data?.pools ?? fallbackPools(target?.stage.stage ?? "A1");
  const selected = data?.items.find((item) => item.symbol === selectedSymbol) ?? data?.items[0] ?? null;
  const totalPages = data ? Math.max(1, Math.ceil(data.total / data.pageSize)) : 1;
  const isA3 = target?.stage.stage.toUpperCase() === "A3";
  const isA2 = target?.stage.stage.toUpperCase() === "A2";
  const stageResultCount = isA2
    ? target?.stage.poolCounts?.effectiveResearch ?? target?.stage.outcome?.counts.effective_research_pool ?? data?.outputCount ?? target?.stage.symbolCount
    : data?.outputCount ?? target?.stage.symbolCount;

  function selectPool(nextPool: StagePoolId): void {
    setPool(nextPool);
    setPage(1);
    setReason("");
    setSelectedSymbol(null);
    setMobileDetailOpen(false);
  }

  function handleTabKeyDown(event: KeyboardEvent<HTMLButtonElement>, index: number): void {
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    event.preventDefault();
    const nextIndex = (index + (event.key === "ArrowRight" ? 1 : -1) + pools.length) % pools.length;
    selectPool(pools[nextIndex].id);
    const tabs = event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>("[role='tab']");
    window.requestAnimationFrame(() => tabs?.[nextIndex]?.focus());
  }

  return (
    <dialog ref={dialogRef} className={`stage-detail-dialog ${mobileDetailOpen ? "stage-detail-mobile-open" : ""}`} aria-labelledby="stage-detail-title" onCancel={(event) => { event.preventDefault(); dialogRef.current?.close(); }} onKeyDown={(event) => { if (event.key === "Escape") { event.preventDefault(); dialogRef.current?.close(); } }}>
      {target ? (
        <div className="stage-detail-shell">
          <header className="stage-detail-header">
            <div className="stage-detail-heading">
              <p className="eyebrow">{target.modelLabel} · {target.stage.stage.toUpperCase()}</p>
              <h2 id="stage-detail-title">{STAGE_LABELS[target.stage.stage.toUpperCase()] ?? target.stage.stage}筛选明细</h2>
              <span>最近一次研究结果</span>
            </div>
            <dl className="stage-detail-metrics">
              <div><dt>状态</dt><dd><StatusBadge status={data?.status ?? target.stage.status} outcome={data?.outcome ?? target.stage.outcome} /></dd></div>
              <div><dt>输入</dt><dd>{progressCount(data?.inputCount)}</dd></div>
              <div><dt>{isA2 ? "有效研究池" : "结果"}</dt><dd>{progressCount(stageResultCount)}</dd></div>
              <div><dt>耗时</dt><dd>{formatDuration(data?.latencyMs ?? target.stage.latencyMs)}</dd></div>
            </dl>
            {data?.outcome ?? target.stage.outcome ? <OutcomeNotice outcome={data?.outcome ?? target.stage.outcome} /> : null}
            <button className="icon-button stage-detail-close" type="button" aria-label="关闭阶段详情" onClick={() => dialogRef.current?.close()}><X size={21} /></button>
          </header>

          <div className="stage-detail-tabs" role="tablist" aria-label="筛选结果分类">
            {pools.map((item, index) => <button key={item.id} id={`stage-tab-${item.id}`} type="button" role="tab" aria-selected={pool === item.id} aria-controls="stage-detail-panel" tabIndex={pool === item.id ? 0 : -1} className={pool === item.id ? "stage-detail-tab stage-detail-tab-active" : "stage-detail-tab"} onClick={() => selectPool(item.id)} onKeyDown={(event) => handleTabKeyDown(event, index)}><span>{item.label}</span><strong>{item.count}</strong></button>)}
          </div>

          <div className="stage-detail-toolbar">
            <label className="stage-detail-search"><Search size={16} aria-hidden="true" /><span className="sr-only">搜索股票代码或名称</span><input value={queryInput} onChange={(event) => setQueryInput(event.target.value)} placeholder="搜索股票代码或名称" /></label>
            <label className="stage-detail-filter"><Filter size={16} aria-hidden="true" /><span className="sr-only">按原因筛选</span><select value={reason} onChange={(event) => { setReason(event.target.value); setPage(1); }}><option value="">全部原因</option>{(data?.reasonOptions ?? []).map((option) => <option key={option} value={option}>{option}</option>)}</select></label>
            <span>{loading ? "读取中…" : `共 ${data?.total ?? 0} 只`}</span>
          </div>

          <div className="stage-detail-content" id="stage-detail-panel" role="tabpanel" aria-labelledby={`stage-tab-${pool}`}>
            <section className="stage-stock-master" aria-label="股票列表">
               <div className="stage-stock-grid stage-stock-grid-head" aria-hidden="true"><span>股票</span><span>主题 / 行业</span><span>{isA3 ? "策略 / 资格" : "评分"}</span><span>主要原因</span><span>状态</span></div>
              <div className="stage-stock-list">
                {error ? <div className="stage-detail-state stage-detail-error"><TriangleAlert size={20} /><strong>明细读取失败</strong><span>{error}</span></div> : loading && !data ? <div className="stage-detail-state"><RefreshCw className="spin" size={20} /><strong>正在读取持久化结果</strong></div> : data?.items.length ? data.items.map((item) => (
                  <button key={item.symbol} type="button" className={item.symbol === selected?.symbol ? "stage-stock-grid stage-stock-row stage-stock-row-selected" : "stage-stock-grid stage-stock-row"} aria-pressed={item.symbol === selected?.symbol} onClick={() => { setSelectedSymbol(item.symbol); setMobileDetailOpen(true); }}>
                    <span className="stage-stock-identity"><strong>{item.name || "名称未提供"}</strong><small>{item.symbol}</small></span>
                    <span>{item.theme || item.industry || "—"}</span>
                     <strong className="stage-stock-score">{isA3 ? <>{strategyProfileLabel(item.plan?.strategyProfile)}<small>{eligibilityLabel(item.plan?.eligibility)}</small></> : item.score === null || item.score === undefined ? "—" : item.score}</strong>
                    <span className="stage-stock-reasons">{humanizeText(item.selectionReasons[0] ?? item.reasonCodes[0] ?? item.evidence[0] ?? "未提供原因")}</span>
                    <span className="stage-stock-result-status"><StatusBadge status={item.status} label={pools.find((entry) => entry.id === item.pool)?.label} />{item.detailState ? <small className={item.detailState === "COMPLETE" ? "detail-completeness detail-complete" : "detail-completeness detail-partial"}>{item.detailState === "COMPLETE" ? "明细完整" : `缺 ${item.missingFields?.length ?? 0} 项`}</small> : null}</span>
                  </button>
                )) : <div className="stage-detail-state"><Database size={20} /><strong>当前筛选条件没有股票</strong><span>可切换分类或清空搜索与原因筛选。</span></div>}
              </div>
              <footer className="stage-stock-pagination"><span>第 {data?.page ?? page} / {totalPages} 页</span><div><button className="icon-button" type="button" aria-label="上一页" disabled={!data || data.page <= 1 || loading} onClick={() => setPage((current) => Math.max(1, current - 1))}><ChevronLeft size={18} /></button><button className="icon-button" type="button" aria-label="下一页" disabled={!data || data.page >= totalPages || loading} onClick={() => setPage((current) => current + 1)}><ChevronRight size={18} /></button></div></footer>
            </section>

             <StageStockDetail item={selected} stage={target?.stage.stage} onBack={() => setMobileDetailOpen(false)} />
          </div>
        </div>
      ) : null}
    </dialog>
  );
}

function detailValue(value: unknown): string {
  return displayValue(value);
}

function strategyProfileLabel(value?: string | null): string {
  if (!value) return "未提供策略";
  const key = value.trim().toUpperCase().replaceAll("-", "_").replaceAll(" ", "_");
  return STRATEGY_PROFILE_LABELS[key] ?? codeLabel(value);
}

function planPriorityLabel(value?: string | null): string {
  const key = value?.trim().toUpperCase();
  return planPriorityText(key);
}

function planPriorityRank(value?: string | null): number {
  const key = value?.trim().toUpperCase();
  return key === "P1" ? 0 : key === "P2" ? 1 : key === "P3" ? 2 : 3;
}

function eligibilityLabel(value?: string | null): string {
  if (!value) return "未提供资格";
  const key = value.trim().toUpperCase().replaceAll("-", "_").replaceAll(" ", "_");
  return ELIGIBILITY_LABELS[key] ?? codeLabel(value);
}

function DetailStringList({ title, badge, values }: { title: string; badge: string; values: string[] }) {
  if (!values.length) return null;
  return <section className="stage-detail-section"><header><h3>{title}</h3><span>{badge}</span></header><ul>{values.map((value, index) => <li key={`${value}-${index}`}>{humanizeText(value)}</li>)}</ul></section>;
}

const DECISION_FACT_LABELS: Record<string, string> = {
  industryChainRole: "产业链角色", marketRole: "市场角色", supplyChainRole: "供应链位置", businessExposure: "业务暴露",
  financialTransmission: "财务传导", capitalFlow: "资金流", tierStructure: "梯队结构", leaderStructure: "龙头结构",
  crowding: "拥挤度", technicalCycle: "技术周期", weeklyConfirmation: "周线确认", indexChainResonance: "指数 / 产业链共振",
};

const MISSING_FIELD_LABELS: Record<string, string> = {
  name: "股票名称", themeOrIndustry: "主题或行业", industry: "行业", score: "阶段评分", selectionReasons: "筛选依据", selectionReasonsOrReasonCodes: "筛选依据或系统原因码", reasonCodes: "系统原因码",
  evidence: "事实证据", risks: "风险说明", invalidation: "失效条件", sourceRefs: "事实来源", lineage: "上游追溯", plan: "A3 计划",
  "decisionFacts.industryChainRole": "产业链角色", "decisionFacts.marketRole": "市场角色", "decisionFacts.supplyChainRole": "供应链位置",
  "decisionFacts.businessExposure": "业务暴露", "decisionFacts.financialTransmission": "财务传导", "decisionFacts.capitalFlow": "资金流",
  "decisionFacts.tierStructure": "梯队结构", "decisionFacts.leaderStructure": "龙头结构", "decisionFacts.crowding": "拥挤度",
  "decisionFacts.technicalCycle": "技术周期", "decisionFacts.weeklyConfirmation": "周线确认", "decisionFacts.indexChainResonance": "指数 / 产业链共振",
};

function StageStockDetail({ item, stage, onBack }: { item: StageDetailItem | null; stage?: string; onBack: () => void }) {
  if (!item) return <aside className="stage-stock-detail"><div className="stage-detail-state"><CircleAlert size={21} /><strong>选择一只股票查看详情</strong><span>模型判断、系统原因码与事实证据会分开展示。</span></div></aside>;
  const isA3 = stage?.toUpperCase() === "A3";
  // A3 uses deterministic route eligibility and conditions; do not surface the
  // legacy composite technical score as if it were part of the new contract.
  const scoreEntries = isA3 ? [] : Object.entries(item.scoreBreakdown ?? {});
  const decisionFactEntries = Object.entries(item.decisionFacts ?? {}).filter(([, value]) => value !== null && value !== undefined);
  const plan = item.plan;
  const hasPlanRoute = Boolean(plan && (plan.strategyProfile || plan.eligibility || plan.noChasePrice !== null && plan.noChasePrice !== undefined || plan.priceDiscovery !== null && plan.priceDiscovery !== undefined));
  return (
    <aside className="stage-stock-detail" aria-label={`${item.symbol} 详情`}>
      <button className="stage-detail-back text-button" type="button" onClick={onBack}><ChevronLeft size={17} />返回股票列表</button>
      <header className="stage-stock-detail-heading"><div><h3>{item.name || "名称未提供"}</h3><span>{stockSymbolLabel(item.symbol)} · {codeLabel(item.theme || item.industry || "行业主题未提供")}</span></div>{!isA3 && item.score !== null && item.score !== undefined ? <strong>{item.score}<small>分</small></strong> : null}</header>
      {item.detailState === "PARTIAL" ? <div className="stage-detail-notice"><CircleAlert size={16} /><div><strong>明细字段不完整</strong><span>未提供：{(item.missingFields ?? []).map((field) => MISSING_FIELD_LABELS[field] ?? fieldLabel(field)).join("、") || "未标明字段"}。页面不会推测填充。</span></div></div> : item.detailState === "COMPLETE" ? <div className="stage-detail-complete-note"><CheckCircle2 size={16} />本阶段要求的股票明细字段完整。</div> : null}
      {item.nameSource === "unavailable" ? <div className="stage-detail-notice"><CircleAlert size={16} />冻结快照和模型结果均未提供名称，页面没有推测填充。</div> : null}
      {item.route || item.bottleneckStatus || item.factorCoverage ? <section className="stage-detail-section"><header><h3>A2 入池通道</h3><span>确定性门禁</span></header><dl className="stage-definition-grid"><div><dt>路线</dt><dd>{detailValue(item.route)}</dd></div><div><dt>瓶颈状态</dt><dd>{detailValue(item.bottleneckStatus)}</dd></div><div><dt>事实覆盖</dt><dd>{detailValue(item.factorCoverage)}</dd></div></dl></section> : null}
      <DetailStringList title="入选逻辑" badge="模型判断" values={item.selectionReasons} />
      <DetailStringList title="淘汰 / 校验原因" badge="系统原因码" values={item.reasonCodes} />
      {decisionFactEntries.length ? <section className="stage-detail-section"><header><h3>关键决策事实</h3><span>持久化事实</span></header><dl className="stage-definition-grid stage-decision-grid">{decisionFactEntries.map(([key, value]) => <div key={key}><dt>{DECISION_FACT_LABELS[key] ?? key}</dt><dd>{detailValue(value)}</dd></div>)}</dl></section> : null}
      {scoreEntries.length ? <section className="stage-detail-section"><header><h3>评分拆解</h3><span>模型字段</span></header><dl className="stage-score-grid">{scoreEntries.map(([key, value]) => <div key={key}><dt>{fieldLabel(key)}</dt><dd>{detailValue(value)}</dd></div>)}</dl></section> : null}
      <DetailStringList title="证据与依据" badge="模型证据" values={item.evidence} />
      {item.sourceRefs.length ? <section className="stage-detail-section"><header><h3>事实来源</h3><span>可追溯来源</span></header><ul className="stage-source-refs">{item.sourceRefs.map((source, index) => <li key={index}>{detailValue(source)}</li>)}</ul></section> : null}
      <DetailStringList title="风险提示" badge="模型风险" values={[...new Set([...item.riskReasons, ...item.risks])]} />
      <DetailStringList title="失效条件" badge="约束条件" values={item.invalidation} />
      {item.lineage && Object.keys(item.lineage).length ? <section className="stage-detail-section"><header><h3>上游追溯</h3><span>来源链路</span></header><dl className="stage-definition-grid">{Object.entries(item.lineage).map(([key, value]) => <div key={key}><dt>{fieldLabel(key)}</dt><dd>{detailValue(value)}</dd></div>)}</dl></section> : null}
       {plan ? <section className="stage-detail-section stage-plan-section">
         <header><h3>A3 技术计划</h3><span>确定性路线 · 只读计划</span></header>
         {hasPlanRoute ? <dl className="stage-definition-grid">
           <div><dt>策略路线</dt><dd>{strategyProfileLabel(plan.strategyProfile)}</dd></div>
           <div><dt>计划优先级</dt><dd>{planPriorityLabel(plan.planPriority)}</dd></div>
           <div><dt>计划资格</dt><dd>{eligibilityLabel(plan.eligibility)}</dd></div>
           <div><dt>禁止追价价位</dt><dd>{detailValue(plan.noChasePrice)}</dd></div>
           <div><dt>价格发现</dt><dd>{plan.priceDiscovery === null || plan.priceDiscovery === undefined ? "—" : plan.priceDiscovery ? "是 · 趋势强度票" : "否"}</dd></div>
         </dl> : null}
         <dl className="stage-definition-grid">
           <div><dt>形态</dt><dd>{detailValue(plan.setupType)}</dd></div>
           <div><dt>参考收盘价</dt><dd>{detailValue(plan.referencePrice)}{plan.referencePriceAsOf ? <small>截至 {plan.referencePriceAsOf}</small> : null}</dd></div>
           <div><dt>触发区间</dt><dd>{plan.triggerZone ? `${detailValue(plan.triggerZone.low)} – ${detailValue(plan.triggerZone.high)}` : "—"}</dd></div>
           <div><dt>失效价</dt><dd>{detailValue(plan.invalidationLevel)}</dd></div>
           <div><dt>压力 / 减仓参考</dt><dd>{detailValue(plan.pressureReducePrice)}{plan.pressureBasis ? <small>{humanizeText(plan.pressureBasis)}</small> : null}</dd></div>
           <div><dt>第一阻力位</dt><dd>{detailValue(plan.firstResistance)}</dd></div>
           <div><dt>盈亏比</dt><dd>{detailValue(plan.rewardRisk)}</dd></div>
           <div><dt>止损距离原始值</dt><dd>{detailValue(plan.stopDistancePct)}</dd></div>
           <div><dt>风险单位</dt><dd>{detailValue(plan.riskUnit)}</dd></div>
           <div><dt>相对强度排名</dt><dd>{detailValue(plan.relativeStrengthRank)}</dd></div>
           <div><dt>真实波幅延伸</dt><dd>{detailValue(plan.atrExtension)}</dd></div>
           <div><dt>最大均线偏离</dt><dd>{detailValue(plan.maBiasMax)}</dd></div>
           <div><dt>禁止追价条件</dt><dd>{detailValue(plan.noChaseCondition)}</dd></div>
           <div><dt>K 线形态</dt><dd>{detailValue(plan.klinePattern)}</dd></div>
           <div><dt>反趋势试探</dt><dd>{detailValue(plan.counterTrendProbe)}</dd></div>
           <div><dt>过度延伸</dt><dd>{detailValue(plan.overExtended)}</dd></div>
           <div><dt>允许时间窗</dt><dd>{detailValue(plan.allowedTimeWindows)}</dd></div>
           <div><dt>均线分析</dt><dd>{detailValue(plan.maAnalysis)}</dd></div>
           <div><dt>计划记录</dt><dd>{plan.planId ? "已生成并落盘" : "—"}</dd></div>
           <div><dt>数据一致性</dt><dd>{plan.planHash && plan.factorSnapshotHash && plan.configHash ? "已绑定研究数据与配置" : "部分校验信息未提供"}</dd></div>
           <div><dt>有效期</dt><dd>{detailValue(plan.planExpiry)}</dd></div>
         </dl>
         {plan.priorityReasons?.length ? <DetailStringList title="优先级依据" badge="确定性档位" values={plan.priorityReasons} /> : null}
         {plan.timeframeStates && Object.keys(plan.timeframeStates).length ? <section className="stage-detail-section"><header><h3>周期状态</h3><span>多周期分析</span></header><dl className="stage-definition-grid">{Object.entries(plan.timeframeStates).map(([key, value]) => <div key={key}><dt>{fieldLabel(key)}</dt><dd>{detailValue(value)}</dd></div>)}</dl></section> : null}
         {plan.scenarios ? <section className="stage-detail-section"><header><h3>情景计划</h3><span>行情应对</span></header><p className="stage-detail-raw-value">{detailValue(plan.scenarios)}</p></section> : null}
         {plan.confirmationConditions?.length ? <DetailStringList title="确认条件" badge="触发约束" values={plan.confirmationConditions} /> : null}
         <DetailStringList title="必备条件" badge="确定性门槛" values={plan.requiredConditions ?? []} />
         <DetailStringList title="已满足条件" badge="确定性事实" values={plan.metConditions ?? []} />
         <DetailStringList title="未满足条件" badge="当前不执行" values={plan.unmetConditions ?? []} />
         <DetailStringList title="模型否决边界" badge="A4 仍需复核" values={plan.vetoConditions ?? []} />
       </section> : null}
    </aside>
  );
}

function MonitorPanel({ overview, onOpen }: { overview: OverviewResponse; onOpen: () => void }) {
  const effective = overview.recentEffectiveEvents.length ? overview.recentEffectiveEvents : overview.monitor.events.filter((event) => event.effective);
  const dispatch = overview.monitor.dispatch;
  return (
    <Panel title="盘中盯盘" icon={<MonitorDot size={18} />} action={<button className="text-button" type="button" onClick={onOpen}>查看详情</button>}>
      <div className="rail-summary"><StatusBadge status={overview.monitor.status} /><span>{formatDateTime(overview.monitor.checkedAt)}</span></div>
      {dispatch && dispatch.status !== "SUCCEEDED_NO_ACTION" && dispatch.status !== "EFFECTIVE_SIGNAL" ? <p className={`monitor-dispatch-inline monitor-dispatch-inline-${dispatch.status.toLowerCase()}`}>{monitorDispatchDetail(dispatch)}</p> : null}
      {effective.length === 0 ? <EmptyState title="暂无有效事件" detail={overview.monitor.activePlanCount ? "A4 正在复核已有计划；继续观察不会写入有效结果。" : "当前没有正式 A3 活动计划，A4 不会自行创建候选。"} icon={<MonitorDot size={21} />} /> : (
        <ul className="event-list">{effective.slice(0, 4).map((event, index) => <EventRow key={`${event.minuteEnd}-${event.laneId}-${index}`} event={event} />)}</ul>
      )}
      <dl className="compact-stats"><div><dt>有效事件</dt><dd>{overview.monitor.effectiveEventCount ?? effective.length}</dd></div><div><dt>活动计划</dt><dd>{overview.monitor.activePlanCount ?? overview.planCounts.ACTIVE_TODAY ?? 0}</dd></div></dl>
    </Panel>
  );
}

function EventRow({ event }: { event: EffectiveEvent }) {
  return <li><div><strong>{monitorActionLabel(event.action)}</strong><span>{event.name ? `${event.name} · ${stockSymbolLabel(event.symbol)}` : stockSymbolLabel(event.symbol)}</span></div><time>{formatDateTime(event.minuteEnd ?? event.time)}</time></li>;
}

const MONITOR_ACTION_LABELS: Record<string, string> = {
  NO_ACTION: "继续观察",
  START_CONFIRMATION: "开始确认",
  BUY_SIGNAL: "模拟入场",
  ADD_SIGNAL: "模拟加仓",
  SELL_SIGNAL: "模拟离场",
  REDUCE_SIGNAL: "模拟减仓",
  FORCED_RISK_EXIT: "硬止损离场",
  LLM_VETO: "模型否决",
  PLAN_INVALIDATED: "计划失效",
  DATA_BLOCK: "数据阻断",
  EMPTY_SCOPE: "空范围",
};

const MONITOR_REASON_LABELS: Record<string, string> = {
  DETERMINISTIC_TRIGGER_PASS: "确定性触发通过，模型未否决",
  DETERMINISTIC_EXIT_TRIGGER: "确定性离场条件触发",
  LLM_VETO: "盘中快速模型否决本次触发",
  HARD_STOP: "价格触及硬止损",
  HARD_STOP_BEFORE_ENTRY: "入场前触及保护位",
  CURRENT_1M_HARD_STOP: "当前一分钟触及保护位",
  A4_FORCED_EXIT_WITHOUT_POSITION: "系统检测到无持仓离场请求，已阻断错误动作",
  A4_BEHAVIOR_TYPE_MISSING: "股票类型尚未确定，不能选择盘中策略",
  BLOCKED_T1: "当日买入暂不可卖，等待下一交易日离场",
  ENTRY_NEXT_BAR_MISSED: "入场信号后的下一根完整分钟线未能成交",
  PRE_ENTRY_RISK_LEVEL_TOUCHED: "入场前触及风险位",
  ENTRY_BLOCKED_CURRENT_MINUTE: "本分钟暂不入场",
  TREND_PRE_ENTRY_STRUCTURE_INVALIDATED: "趋势策略入场前结构失效",
  MA520_PRE_ENTRY_STRUCTURE_INVALIDATED: "五日与二十日均线入场前结构失效",
  TREND_5M_FAILED_MA5_RECLAIM: "趋势五日线策略跌破五日线且回抽失败",
  TREND_HIGH_VOLUME_MA5_BREAK: "趋势五日线策略放量跌破五日线",
  TREND_HIGH_VOLUME_UPPER_SHADOW: "趋势五日线策略高位放量长上影",
  MA520_5M_FAILED_MA20_RECLAIM: "五二零策略跌破二十日线且回抽失败",
  MA520_HIGH_VOLUME_MA20_BREAK: "五二零策略放量跌破二十日线",
  MINUTE_DATA_GAP: "分钟线存在缺口",
  MINUTE_DATA_UNAVAILABLE: "当前分钟线不可用",
  BAR_MISSING: "计划股票缺少当前分钟线",
  BAR_NOT_CURRENT_1M: "不是当前闭合1分钟K线",
  MONITOR_OVERRUN: "本分钟处理超时",
  LLM_UNAVAILABLE: "盯盘模型不可用",
  PLAN_INVALIDATED: "A3计划已失效",
  DETERMINISTIC_STRATEGY_CONFIRMATION: "策略条件已满足，等待模型复核",
  STRATEGY_TRIGGER_NOT_MET: "策略触发条件尚未满足",
  PLAN_DATA_GAP: "计划数据不足",
  NO_CLOSED_5M: "缺少闭合5分钟线",
  NO_CLOSED_15M: "缺少闭合15分钟结构",
  EMPTY_SCOPE: "当前没有今日活动计划",
  MINUTE_CACHE_CONFLICT: "分钟缓存发生版本冲突",
  DATA_BLOCK: "数据门禁阻断本次检查",
};

function monitorActionLabel(action?: string | null): string {
  return action ? MONITOR_ACTION_LABELS[action] ?? codeLabel(action) : "有效事件";
}

function monitorReasonLabel(reason?: string | null): string {
  return reason ? MONITOR_REASON_LABELS[reason] ?? codeLabel(reason) : "未提供原因";
}

function monitorDispatchLabel(status?: string | null): string {
  return statusLabel(status);
}

function monitorDispatchDetail(dispatch?: MonitorDispatchSummary | null): string {
  if (!dispatch) return "尚未读取到 A4 调度结果。";
  const attemptTime = dispatch.latestAttemptAt ? `最近尝试 ${formatDateTime(dispatch.latestAttemptAt)}` : "";
  if (dispatch.status === "RUNNING") return `本分钟任务正在执行${dispatch.latestRunId ? `（${dispatch.latestRunId}）` : ""}${attemptTime ? `，${attemptTime}` : ""}。`;
  if (dispatch.status === "FAILED") {
    const reason = dispatch.lastReasonCode ? `原因：${monitorReasonLabel(dispatch.lastReasonCode)}` : "原因未提供";
    const symbols = dispatch.affectedSymbols?.length ? `，影响 ${dispatch.affectedSymbols.join("、")}` : "";
    const failedAt = dispatch.lastFailureAt ?? dispatch.latestCompletedAt ?? dispatch.latestAttemptAt;
    return `最近一次 A4 调度失败（${formatDateTime(failedAt)}），${reason}${symbols}。失败不会被显示为空范围。`;
  }
  if (dispatch.status === "DATA_BLOCK") return `任务已执行（${formatDateTime(dispatch.latestCompletedAt ?? dispatch.latestAttemptAt)}），但分钟线或计划事实未通过数据门禁；未产生交易信号。`;
  if (dispatch.status === "EMPTY_SCOPE") return `任务已执行（${formatDateTime(dispatch.latestCompletedAt ?? dispatch.latestAttemptAt)}），但当前没有今日活动计划；下一日待复核计划不会进入本次盯盘。`;
  if (dispatch.status === "EFFECTIVE_SIGNAL") return `任务已执行并产生有效盯盘事件（${formatDateTime(dispatch.latestCompletedAt ?? dispatch.latestAttemptAt)}），详见下方正式实时信号。`;
  if (dispatch.status === "SUCCEEDED_NO_ACTION") return `任务已完成（${formatDateTime(dispatch.lastSuccessAt ?? dispatch.latestCompletedAt ?? dispatch.latestAttemptAt)}），本分钟确定性条件未触发有效动作。`;
  return "尚未形成可判定的 A4 调度状态。";
}

function monitorPlanNames(plans: MonitorPlan[] | undefined): string {
  if (!plans?.length) return "—";
  const values = plans.slice(0, 12).map((plan) => `${plan.name || "名称未提供"}（${stockSymbolLabel(plan.symbol)}）`);
  return plans.length > values.length ? `${values.join("、")} 等 ${plans.length} 只` : values.join("、");
}

function larkNotificationKindLabel(kind?: string | null): string {
  if (kind === "PREMARKET_A3_ANALYSIS") return "A3 盘前分析";
  if (kind === "PREMARKET_A3") return "盘前 A3 计划";
  if (kind === "A4_EFFECTIVE") return "A4 有效事件";
  if (kind === "A5_MIDDAY_REVIEW") return "A5 盘中复盘";
  if (kind === "A5_POST_CLOSE_REVIEW") return "A5 盘后复盘";
  return "系统通知";
}

function larkNotificationStatusLabel(status?: string | null): string {
  if (status === "SENT") return "已送达";
  if (status === "FAILED") return "发送失败";
  return "状态未知";
}

function larkColorClass(color?: string | null): string {
  const allowed = new Set(["blue", "wathet", "turquoise", "green", "yellow", "orange", "red", "carmine", "violet", "purple", "indigo", "lime", "grey"]);
  return allowed.has(color || "") ? `lark-color-${color}` : "lark-color-grey";
}

function LarkNotificationPanel({ notifications }: { notifications: LarkNotificationSummary[] }) {
  const [settings, setSettings] = useState<LarkSettingsStatus | null>(null);
  const [webhookUrl, setWebhookUrl] = useState("");
  const [saving, setSaving] = useState(false);
  const [feedback, setFeedback] = useState<{ tone: "success" | "error"; message: string } | null>(null);

  useEffect(() => {
    let disposed = false;
    void apiFetch<LarkSettingsStatus>("/api/settings/lark")
      .then((status) => { if (!disposed) setSettings(status); })
      .catch((error: unknown) => { if (!disposed) setFeedback({ tone: "error", message: error instanceof Error ? humanizeText(error.message) : "无法读取飞书配置" }); });
    return () => { disposed = true; };
  }, []);

  async function saveWebhook(event: FormEvent): Promise<void> {
    event.preventDefault();
    setSaving(true);
    setFeedback(null);
    try {
      const status = await apiMutation<LarkSettingsStatus>("/api/settings/lark", {
        method: "PUT",
        body: JSON.stringify({ webhookUrl }),
      });
      setSettings(status);
      setWebhookUrl("");
      setFeedback({ tone: "success", message: "机器人地址已安全保存，下一次 A3、A4 或 A5 推送立即生效。" });
    } catch (error) {
      setFeedback({ tone: "error", message: error instanceof Error ? humanizeText(error.message) : "保存失败" });
    } finally {
      setSaving(false);
    }
  }

  async function clearWebhook(): Promise<void> {
    setSaving(true);
    setFeedback(null);
    try {
      const status = await apiMutation<LarkSettingsStatus>("/api/settings/lark", { method: "DELETE" });
      setSettings(status);
      setWebhookUrl("");
      setFeedback({ tone: "success", message: "机器人地址已移除，研究与盯盘仍会正常运行，但不再发送飞书通知。" });
    } catch (error) {
      setFeedback({ tone: "error", message: error instanceof Error ? humanizeText(error.message) : "移除失败" });
    } finally {
      setSaving(false);
    }
  }

  return <Panel title="飞书推送记录" icon={<Send size={18} />} action={<span className="lark-delivery-note">仅展示安全摘要</span>}>
    <div className="lark-settings">
      <div className="lark-settings-status">
        <span className={settings?.configured ? "lark-configured" : "lark-unconfigured"}>{settings === null ? "读取中" : settings.configured ? "已配置" : "未配置"}</span>
        <strong>{settings?.masked ?? "机器人地址不会回传到浏览器"}</strong>
        <small>{settings?.updatedAt ? `${formatDateTime(settings.updatedAt)} 更新` : "保存在服务器本机，不写入环境变量文件、日志或推送记录"}</small>
      </div>
      <form className="lark-settings-form" onSubmit={(event) => void saveWebhook(event)}>
        <label htmlFor="lark-webhook">飞书自定义机器人地址</label>
        <div className="lark-settings-controls">
          <input id="lark-webhook" type="password" autoComplete="off" spellCheck={false} placeholder={settings?.configured ? "输入新地址以替换现有配置" : "https://open.larksuite.com/open-apis/bot/v2/hook/…"} value={webhookUrl} onChange={(event) => setWebhookUrl(event.target.value)} required />
          <button className="primary-button" type="submit" disabled={saving || !webhookUrl.trim()}>{saving ? "保存中" : settings?.configured ? "替换" : "保存"}</button>
          {settings?.configured ? <button className="secondary-button" type="button" onClick={() => void clearWebhook()} disabled={saving}>移除</button> : null}
        </div>
        {feedback ? <p className={`lark-settings-feedback lark-settings-feedback-${feedback.tone}`} role={feedback.tone === "error" ? "alert" : "status"}>{feedback.message}</p> : null}
      </form>
    </div>
    {notifications.length === 0 ? <EmptyState title="暂无推送记录" detail="A3 盘前研究、A4 有效状态变化和 A5 每日复盘完成后，这里会显示投递结果；普通观察状态不会推送。" icon={<Send size={22} />} /> : <div className="data-table-wrap"><table className="data-table lark-delivery-table"><thead><tr><th>时间</th><th>类型</th><th>卡片</th><th>状态</th><th>尝试</th><th>异常</th></tr></thead><tbody>{notifications.map((item, index) => <tr key={item.deliveryId || `${item.sourceId}-${index}`}><td>{formatDateTime(item.sentAt ?? item.updatedAt ?? item.createdAt)}</td><td>{larkNotificationKindLabel(item.kind)}</td><td><span className="lark-card-title"><i className={larkColorClass(item.color)} aria-hidden="true" /><strong>{humanizeText(item.title || "未命名通知")}</strong></span></td><td><span className={`lark-delivery-status lark-delivery-status-${(item.status || "unknown").toLowerCase()}`}>{larkNotificationStatusLabel(item.status)}</span></td><td>{item.attemptCount ?? 0}</td><td className="monitor-reason">{item.lastReasonCode ? codeLabel(item.lastReasonCode) : "—"}</td></tr>)}</tbody></table></div>}
  </Panel>;
}

function priceRange(plan?: MonitorPlan | null): string {
  if (plan?.triggerLow === null || plan?.triggerLow === undefined || plan?.triggerHigh === null || plan?.triggerHigh === undefined) return "—";
  return `${detailValue(plan.triggerLow)} – ${detailValue(plan.triggerHigh)}`;
}

function MonitorEventDialog({ event, onClose }: { event: EffectiveEvent | null; onClose: () => void }) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const dialog = ref.current;
    if (!dialog) return;
    if (event && !dialog.open) dialog.showModal();
    if (!event && dialog.open) dialog.close();
  }, [event]);
  const plan = event?.plan;
  const strategyProfile = event?.strategyProfile ?? plan?.strategyProfile;
  const eligibility = event?.eligibility ?? plan?.eligibility;
  const hasPlanConditions = Boolean(plan && [plan.requiredConditions, plan.metConditions, plan.unmetConditions, plan.vetoConditions].some((values) => values?.length));
  return <dialog ref={ref} className="monitor-detail-dialog" onClose={onClose} onCancel={(e) => { e.preventDefault(); onClose(); }}>
    {event ? <div className="monitor-detail-shell">
      <header className="monitor-detail-header"><div><span className="monitor-detail-mode">{event.testOnly ? "测试回放信号" : "正式模拟信号"}</span><h2>{event.name || "名称未提供"}<small>{stockSymbolLabel(event.symbol)}</small></h2><p>{monitorActionLabel(event.action)} · {formatDateTime(event.minuteEnd ?? event.time)}</p></div><button className="icon-button" type="button" aria-label="关闭信号详情" onClick={onClose}><X size={19} /></button></header>
      <div className="monitor-detail-body">
         <section className="monitor-detail-summary"><div><span>动作</span><strong>{monitorActionLabel(event.action)}</strong></div><div><span>触发原因</span><strong>{monitorReasonLabel(event.reasonCode)}</strong>{event.diagnosticCode ? <small>{codeLabel(event.diagnosticCode)}</small> : null}</div><div><span>策略</span><strong>{strategyProfileLabel(strategyProfile)}</strong></div><div><span>资格</span><strong>{eligibilityLabel(eligibility)}</strong></div><div><span>研究模型</span><strong>{MODEL_LABELS[event.laneId ?? ""] ?? modelNameLabel(event.laneId)}</strong></div><div><span>模拟结果</span><strong>{event.simulation?.status === "FILLED" ? "已成交" : event.action === "LLM_VETO" ? "已否决" : "未成交 / 不适用"}</strong></div></section>
         <section className="monitor-detail-section"><header><h3>A3 计划约束</h3><span>只读</span></header>{plan ? <dl className="monitor-detail-grid"><div><dt>计划标识</dt><dd>{plan.planId || event.planId ? "已生成" : "—"}</dd></div><div><dt>优先级</dt><dd>{planPriorityLabel(plan.planPriority)}</dd></div><div><dt>策略路线</dt><dd>{strategyProfileLabel(strategyProfile)}</dd></div><div><dt>计划资格</dt><dd>{eligibilityLabel(eligibility)}</dd></div><div><dt>形态</dt><dd>{detailValue(plan.setupType)}</dd></div><div><dt>参考收盘价</dt><dd>{detailValue(plan.referencePrice)}</dd></div><div><dt>触发区间</dt><dd>{priceRange(plan)}</dd></div><div><dt>失效价</dt><dd>{detailValue(plan.stopLevel)}</dd></div><div><dt>禁止追价价位</dt><dd>{detailValue(plan.noChasePrice)}</dd></div><div><dt>压力 / 减仓参考</dt><dd>{detailValue(plan.pressureReducePrice)}</dd></div><div><dt>风险单位</dt><dd>{detailValue(plan.riskUnit)}</dd></div><div><dt>有效期</dt><dd>{formatDateTime(plan.expiresAt)}</dd></div></dl> : <p className="monitor-detail-empty">该事件没有可用的计划明细，页面不会推测补齐。</p>}</section>
         {hasPlanConditions ? <section className="monitor-detail-section"><header><h3>确定性条件</h3><span>十五分钟 / 五分钟</span></header><dl className="monitor-detail-grid"><div><dt>必备条件</dt><dd>{detailValue(plan?.requiredConditions)}</dd></div><div><dt>已满足条件</dt><dd>{detailValue(plan?.metConditions)}</dd></div><div><dt>未满足条件</dt><dd>{detailValue(plan?.unmetConditions)}</dd></div><div><dt>模型否决边界</dt><dd>{detailValue(plan?.vetoConditions)}</dd></div></dl></section> : null}
        {plan?.selectionReasons?.length ? <section className="monitor-detail-section"><header><h3>入选依据</h3><span>持久化结果</span></header><ul>{plan.selectionReasons.map((reason, index) => <li key={`${reason}-${index}`}>{humanizeText(reason)}</li>)}</ul></section> : null}
        <section className="monitor-detail-section"><header><h3>模拟成交</h3><span>内部账户</span></header>{event.simulation ? <dl className="monitor-detail-grid"><div><dt>状态</dt><dd>{codeLabel(event.simulation.status)}</dd></div><div><dt>方向</dt><dd>{codeLabel(event.simulation.action)}</dd></div><div><dt>数量</dt><dd>{detailValue(event.simulation.qty)}</dd></div><div><dt>价格</dt><dd>{detailValue(event.simulation.price)}</dd></div><div><dt>费用</dt><dd>{detailValue(event.simulation.fee)}</dd></div><div><dt>成交分钟线</dt><dd>{formatDateTime(event.simulation.barEnd)}</dd></div></dl> : <p className="monitor-detail-empty">该事件未产生模拟成交；模型否决、数据阻断和计划失效均不会下单。</p>}</section>
      </div>
    </div> : null}
  </dialog>;
}

function lifecyclePercent(value?: number | null): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  // Runtime stores returns as decimal ratios (0.005 = 0.50%).
  return `${(value * 100).toFixed(2)}%`;
}

function lifecyclePrice(value?: number | null): string {
  return value === null || value === undefined || !Number.isFinite(value) ? "—" : value.toFixed(2);
}

function lifecycleQuantity(value?: number | null): string {
  return value === null || value === undefined || !Number.isFinite(value) ? "—" : `${Math.max(0, Math.floor(value))} 股`;
}

function lifecycleR(value?: number | null): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return `${value.toFixed(2)}R`;
}

function lifecycleStatusCount(
  lifecycles: A4SignalLifecycle[],
  counts: Record<string, number>,
  status: string,
): number {
  const value = counts[status];
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : lifecycles.filter((item) => item.status?.trim().toUpperCase() === status).length;
}

function LifecycleTimestamp({ value, price }: { value?: string | null; price?: number | null }) {
  return <span className="lifecycle-value"><strong>{formatDateTime(value)}</strong>{price !== null && price !== undefined ? <small>{lifecyclePrice(price)}</small> : null}</span>;
}

function A4SignalLifecyclePanel({ lifecycles, counts }: { lifecycles: A4SignalLifecycle[]; counts: Record<string, number> }) {
  const signalled = lifecycleStatusCount(lifecycles, counts, "SIGNALLED");
  const open = lifecycleStatusCount(lifecycles, counts, "OPEN");
  const closed = lifecycleStatusCount(lifecycles, counts, "CLOSED");
  const unfilled = lifecycleStatusCount(lifecycles, counts, "UNFILLED");
  return (
    <Panel title="信号跟踪与复盘" icon={<Activity size={18} />} action={<span className="lifecycle-readonly-note">只读记录 · 自动更新</span>}>
      <div className="lifecycle-summary" aria-label="信号生命周期摘要">
        <div><span>最近记录</span><strong>{lifecycles.length}</strong></div>
        <div><span>已发出信号</span><strong>{signalled}</strong></div>
        <div><span>持仓中</span><strong>{open}</strong></div>
        <div><span>已完成</span><strong>{closed}</strong></div>
        <div><span>未成交</span><strong>{unfilled}</strong></div>
      </div>
      {lifecycles.length === 0 ? (
        <EmptyState title="尚无明确入场信号" detail="A4 只有在三种策略的确定性条件满足、且未被盘中模型否决时才会记录入场信号；继续观察不会生成样本。" icon={<Activity size={22} />} />
      ) : (
        <div className="data-table-wrap lifecycle-table-wrap">
          <table className="data-table lifecycle-table">
            <caption className="sr-only">A4 入场信号、模拟成交、持仓过程和离场复盘记录</caption>
            <thead><tr><th>股票</th><th>类型 / 策略</th><th>生命周期</th><th>入场信号 / 成交</th><th>当前持仓 / 离场</th><th>最大浮盈 / 最大回撤</th><th>净收益 / 风险收益倍数</th><th>离场原因</th><th>更新时间</th></tr></thead>
            <tbody>{lifecycles.map((item, index) => (
              <tr key={item.lifecycleId || `${item.symbol}-${item.entrySignalAt || item.updatedAt || index}`}>
                <td><span className="monitor-stock"><strong>{item.name || "名称未提供"}</strong><small>{stockSymbolLabel(item.symbol)}</small></span></td>
                <td><span className="lifecycle-type"><strong>{codeLabel(item.stockBehaviorType)}</strong><small>{strategyProfileLabel(item.strategyProfile)}</small></span></td>
                <td><span className={`lifecycle-status lifecycle-status-${(item.status || "unknown").toLowerCase()}`}><strong>{codeLabel(item.status)}</strong>{item.dataQualityStatus ? <small>数据：{codeLabel(item.dataQualityStatus)}</small> : null}</span></td>
                <td><div className="lifecycle-dual-value"><div><span>信号</span><LifecycleTimestamp value={item.entrySignalAt} price={item.entrySignalPrice} /></div><div><span>成交</span><LifecycleTimestamp value={item.entryFillAt} price={item.entryFillPrice} />{item.entryQty !== null && item.entryQty !== undefined ? <small>{lifecycleQuantity(item.entryQty)}</small> : null}</div></div></td>
                <td><div className="lifecycle-dual-value"><div><span>当前</span><strong>{lifecycleQuantity(item.currentQty)}</strong></div><div><span>信号</span><LifecycleTimestamp value={item.exitSignalAt} /></div><div><span>成交</span><LifecycleTimestamp value={item.exitFillAt} price={item.exitFillPrice} />{item.exitQty !== null && item.exitQty !== undefined ? <small>{lifecycleQuantity(item.exitQty)}</small> : null}</div></div></td>
                <td><div className="lifecycle-metric-pair"><span>最大浮盈 {lifecyclePercent(item.mfePct)}</span><span>最大回撤 {lifecyclePercent(item.maePct)}</span><small>区间 {lifecyclePrice(item.minPrice)} – {lifecyclePrice(item.maxPrice)}</small></div></td>
                <td><div className="lifecycle-metric-pair"><strong>{lifecyclePercent(item.netReturnPct)}</strong><span>毛收益 {lifecyclePercent(item.grossReturnPct)}</span><span>实现盈亏 {formatMoney(item.realizedPnl)}</span><small>{lifecycleR(item.rMultiple)}</small></div></td>
                <td className="lifecycle-exit-reason">{item.exitReasonCode ? codeLabel(item.exitReasonCode) : "尚未离场"}</td>
                <td><time>{formatDateTime(item.updatedAt)}</time>{item.holdingMinutes !== null && item.holdingMinutes !== undefined ? <small className="lifecycle-holding">历时 {item.holdingMinutes} 分钟</small> : null}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}
      <div className="panel-footnote"><ShieldCheck size={14} /><span>入场信号与后续成交、离场、盈亏数据会持续保留，用于区分策略质量、数据异常和未成交情况；此处不提供交易操作。</span></div>
    </Panel>
  );
}

function DataSourcesPanel({ sources, onOpen }: { sources: DataSourceSummary[]; onOpen: () => void }) {
  const labels: Record<string, string> = { MODEL_GATEWAY: "模型网关", HITHINK: "同花顺", MOOTDX: "通达信分钟线" };
  return (
    <Panel title="数据源" icon={<Database size={18} />} action={<button className="text-button" type="button" onClick={onOpen}>部署状态</button>}>
      {sources.length === 0 ? <EmptyState title="暂无探针结果" detail="完成能力探针后，这里会显示各事实源的最新状态。" icon={<Database size={21} />} /> : (
        <ul className="source-list">{sources.slice(0, 6).map((source) => (
          <li key={source.id}><span>{labels[source.label.toUpperCase()] ?? humanizeText(source.label)}</span><StatusBadge status={source.status} /><time>{formatDateTime(source.checkedAt)}</time></li>
        ))}</ul>
      )}
    </Panel>
  );
}

const ISSUE_SEVERITY_LABELS: Record<WorkbenchIssueSeverity, string> = { CRITICAL: "严重", WARNING: "警告", INFO: "提示" };
const ISSUE_SOURCE_LABELS: Record<WorkbenchIssue["source"], string> = { DEPLOYMENT: "部署", WORKFLOW: "工作流", DATA_SOURCE: "数据源", RUNTIME: "运行时", PLAN: "计划" };

function IssueSeverityBadge({ severity }: { severity: WorkbenchIssueSeverity }) {
  return <span className={`issue-severity issue-${severity.toLowerCase()}`}><span aria-hidden="true" />{ISSUE_SEVERITY_LABELS[severity]}</span>;
}

function issueLocation(issue: WorkbenchIssue): string {
  const details = [issue.runId ? "相关研究运行" : null, issue.laneId ? "相关研究模型" : null, issue.stage ? progressPhaseLabel(issue.stage) : null].filter(Boolean);
  return details.join(" · ") || ISSUE_SOURCE_LABELS[issue.source];
}

function IssuePanel({ issues, onOpen }: { issues: WorkbenchIssue[]; onOpen: () => void }) {
  const open = issues.filter((issue) => issue.status === "OPEN");
  const visible = (open.length ? open : issues).slice(0, 5);
  return (
    <Panel title="问题跟踪" icon={<CircleAlert size={18} />} action={<button className="text-button" type="button" onClick={onOpen}>查看全部问题</button>}>
      {visible.length === 0 ? (
        <EmptyState title="当前没有待跟踪问题" detail="工作流、数据源、部署门禁与近期运行日志均未报告异常。" icon={<CheckCircle2 size={22} />} />
      ) : (
        <div className="issue-list">
          {visible.map((issue) => <article key={issue.id} className="issue-row">
            <IssueSeverityBadge severity={issue.severity} />
            <div><strong>{issue.title}</strong><span>{humanizeText(issue.detail)}</span><small>{issueLocation(issue)} · {codeLabel(issue.code)}</small></div>
            <div className="issue-row-meta"><span>{issue.status === "OPEN" ? "待处理" : "观察中"}</span><time>{formatDateTime(issue.lastSeenAt)}</time></div>
          </article>)}
        </div>
      )}
      <div className="panel-footnote"><CircleDotDashed size={14} /><span>待处理来自当前持久化状态；观察中来自近期日志或当前无计划状态，恢复后会自动退出当前清单。</span></div>
    </Panel>
  );
}

function IssuesPage({ issues }: { issues: WorkbenchIssue[] }) {
  const [severity, setSeverity] = useState<"ALL" | WorkbenchIssueSeverity>("ALL");
  const [status, setStatus] = useState<"ALL" | WorkbenchIssue["status"]>("ALL");
  const filtered = useMemo(() => issues.filter((issue) => (severity === "ALL" || issue.severity === severity) && (status === "ALL" || issue.status === status)), [issues, severity, status]);
  const counts = {
    open: issues.filter((issue) => issue.status === "OPEN").length,
    critical: issues.filter((issue) => issue.severity === "CRITICAL").length,
    warning: issues.filter((issue) => issue.severity === "WARNING").length,
    observing: issues.filter((issue) => issue.status === "OBSERVING").length,
  };
  return <div className="page-stack">
    <PageHeading eyebrow="全流程可追溯" title="问题跟踪" detail="统一汇总部署门禁、工作流验收、数据源健康度与近期运行异常；当前状态恢复后会自动从待处理清单移除。" />
    <div className="summary-strip"><SummaryItem label="待处理" value={String(counts.open)} /><SummaryItem label="严重" value={String(counts.critical)} /><SummaryItem label="警告" value={String(counts.warning)} /><SummaryItem label="观察中" value={String(counts.observing)} /></div>
    <Panel title="当前问题" icon={<CircleAlert size={18} />}>
      <div className="issue-toolbar">
        <label><span>严重度</span><select value={severity} onChange={(event) => setSeverity(event.target.value as "ALL" | WorkbenchIssueSeverity)}><option value="ALL">全部</option><option value="CRITICAL">严重</option><option value="WARNING">警告</option><option value="INFO">提示</option></select></label>
        <label><span>状态</span><select value={status} onChange={(event) => setStatus(event.target.value as "ALL" | WorkbenchIssue["status"])}><option value="ALL">全部</option><option value="OPEN">待处理</option><option value="OBSERVING">观察中</option></select></label>
        <span>{filtered.length} 项</span>
      </div>
      {filtered.length === 0 ? <EmptyState title="当前筛选条件下没有问题" detail="可切换严重度或状态查看其它项目。" icon={<CheckCircle2 size={22} />} /> : <div className="data-table-wrap"><table className="data-table issue-table"><thead><tr><th>严重度</th><th>状态</th><th>问题</th><th>定位</th><th>首次 / 最近</th><th>次数</th></tr></thead><tbody>{filtered.map((issue) => <tr key={issue.id}>
        <td><IssueSeverityBadge severity={issue.severity} /></td>
        <td><span className={`issue-status issue-status-${issue.status.toLowerCase()}`}>{issue.status === "OPEN" ? "待处理" : "观察中"}</span></td>
        <td className="issue-description"><strong>{issue.title}</strong><span>{humanizeText(issue.detail)}</span><code>{codeLabel(issue.code)}</code></td>
        <td className="mono-cell">{issueLocation(issue)}</td>
        <td><time>{formatDateTime(issue.firstSeenAt)}</time><span className="issue-time-separator"> / </span><time>{formatDateTime(issue.lastSeenAt)}</time></td>
        <td>{issue.occurrenceCount}</td>
      </tr>)}</tbody></table></div>}
      <div className="panel-footnote"><ShieldCheck size={14} /><span>该页面只做只读聚合，不会自动重试、重启、放宽研究门槛或连接外部交易。</span></div>
    </Panel>
  </div>;
}

function LogPanel({ logs, onOpen }: { logs: LogEntry[]; onOpen: () => void }) {
  return (
    <Panel title="实时日志" icon={<ScrollText size={18} />} action={<button className="text-button" type="button" onClick={onOpen}>查看全部日志</button>}>
      <LogTable logs={logs} compact />
    </Panel>
  );
}

function LogTable({ logs, compact = false }: { logs: LogEntry[]; compact?: boolean }) {
  if (!logs.length) return <EmptyState title="暂无控制台日志" detail="服务启动任务后，脱敏后的运行过程会显示在这里。" icon={<ScrollText size={21} />} />;
  return (
    <div className="log-table-wrap" aria-live="off">
      <table className="log-table"><thead><tr><th>时间</th><th>级别</th><th>任务</th><th>消息</th><th>耗时</th></tr></thead>
        <tbody>{logs.slice(0, compact ? 10 : undefined).map((entry, index) => (
          <tr key={entry.id ?? `${entry.timestamp}-${index}`}>
            <td><time>{formatDateTime(entry.timestamp)}</time></td>
            <td><span className={`log-level level-${entry.level.toLowerCase()}`}>{logLevelLabel(entry.level)}</span></td>
            <td>{jobLabel(entry.job)}</td><td className="log-message">{humanizeText(entry.message)}</td><td>{formatDuration(entry.durationMs)}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  );
}

function FunnelPage({ overview, runs }: { overview: OverviewResponse; runs: RunSummary[] }) {
  return (
    <div className="page-stack">
      <PageHeading eyebrow="A1 → A2 → A3" title="研究漏斗" detail="每个研究模型严格独立；上游未验证时，下游不会运行。" />
      <WorkflowProgressPanel progress={overview.workflowProgress} />
      <FunnelPanel workflow={overview.latestWorkflow} onOpen={() => undefined} />
      <Panel title="最近运行" icon={<FileClock size={18} />}>
        {runs.length === 0 ? <EmptyState title="暂无运行历史" detail="首次运行完成后，这里会按时间列出研究和盯盘任务。" /> : <RunsTable runs={runs} />}
      </Panel>
    </div>
  );
}

function RunsTable({ runs }: { runs: RunSummary[] }) {
  return <div className="data-table-wrap"><table className="data-table"><thead><tr><th>运行记录</th><th>任务</th><th>时段</th><th>状态</th><th>完成时间</th><th>耗时</th></tr></thead><tbody>{runs.map((run, index) => (
    <tr key={run.runId}><td className="mono-cell">第 {index + 1} 条</td><td>{jobLabel(run.job)}</td><td>{slotLabel(run.slot)}</td><td><StatusBadge status={run.status} /></td><td>{formatDateTime(run.finishedAt ?? run.updatedAt ?? (run.mtimeMs ? new Date(run.mtimeMs).toISOString() : null))}</td><td>{formatDuration(run.durationMs)}</td></tr>
  ))}</tbody></table></div>;
}

function MonitorPage({ overview }: { overview: OverviewResponse }) {
  const events = overview.recentEffectiveEvents.length ? overview.recentEffectiveEvents : overview.monitor.events.filter((event) => event.effective);
  const plans = overview.monitor.plans ?? [];
  const activePlans = overview.monitor.activePlans ?? plans.filter((plan) => plan.status === "ACTIVE_TODAY");
  const pendingPlans = overview.monitor.pendingPlans ?? plans.filter((plan) => plan.status === "PENDING_MORNING_REVIEW");
  const latestA3Plans = overview.monitor.latestA3Plans ?? plans;
  const activePlanCount = overview.monitor.activePlanCount ?? overview.planCounts.ACTIVE_TODAY ?? activePlans.length;
  const pendingPlanCount = overview.monitor.pendingPlanCount ?? overview.planCounts.PENDING_MORNING_REVIEW ?? pendingPlans.length;
  const filledCount = events.filter((event) => event.simulation?.status === "FILLED").length;
  const [selectedEvent, setSelectedEvent] = useState<EffectiveEvent | null>(null);
  const replay = overview.monitor.replay;
  const dispatch = overview.monitor.dispatch;
  const dispatchStatus = dispatch?.status ?? overview.monitor.status;
  const notifications = overview.monitor.notifications ?? [];
  return <div className="page-stack"><PageHeading eyebrow="A4 · 模型仅可否决" title="盘中盯盘" detail="确定性规则先触发，盘中复核模型只允许否决；全部结果进入本地模拟账户，不连接真实交易。" />
    <div className="summary-strip monitor-summary-strip"><SummaryItem label="最新检查" value={formatDateTime(overview.monitor.checkedAt)} /><SummaryItem label="正式活动计划" value={String(activePlanCount)} /><SummaryItem label="下一日待复核" value={String(pendingPlanCount)} /><SummaryItem label="有效事件 / 模拟成交" value={`${overview.monitor.effectiveEventCount ?? events.length} / ${filledCount}`} /><SummaryItem label="当前状态" value={monitorDispatchLabel(dispatchStatus)} /></div>
    <section className={`monitor-readiness ${dispatchStatus === "FAILED" || dispatchStatus === "DATA_BLOCK" ? "monitor-readiness-blocked" : ""}`}>
      {dispatchStatus === "FAILED" || dispatchStatus === "DATA_BLOCK" || dispatchStatus === "EMPTY_SCOPE" ? <TriangleAlert size={19} /> : dispatchStatus === "RUNNING" ? <CircleDotDashed size={19} /> : <CheckCircle2 size={19} />}
      <div><strong>A4 调度：{monitorDispatchLabel(dispatchStatus)}</strong><p>{monitorDispatchDetail(dispatch)}</p></div>
    </section>

    <Panel title="正式实时信号" icon={<MonitorDot size={18} />}>
      {events.length === 0 ? <EmptyState title="目前没有有效盯盘事件" detail={activePlanCount ? "这是正常结果。系统不会把每分钟的继续观察写入最终结果。" : "当前没有正式活动计划，系统不会自行创建候选。"} icon={<MonitorDot size={22} />} /> : <div className="data-table-wrap"><table className="data-table monitor-event-table"><thead><tr><th>时间</th><th>股票</th><th>策略</th><th>信号</th><th>模拟结果</th><th>原因</th><th><span className="sr-only">详情</span></th></tr></thead><tbody>{events.map((event, index) => <tr key={`${event.minuteEnd}-${event.planId}-${index}`}><td>{formatDateTime(event.minuteEnd ?? event.time)}</td><td><span className="monitor-stock"><strong>{event.name || "名称未提供"}</strong><small>{stockSymbolLabel(event.symbol)}</small></span></td><td>{strategyProfileLabel(event.strategyProfile ?? event.plan?.strategyProfile)}</td><td><span className={`monitor-action monitor-action-${(event.action || "unknown").toLowerCase()}`}>{monitorActionLabel(event.action)}</span></td><td>{event.simulation?.status === "FILLED" ? <span className="monitor-fill-status monitor-fill-success">已成交 · {detailValue(event.simulation.qty)}股</span> : event.action === "LLM_VETO" ? <span className="monitor-fill-status">未成交 · 模型否决</span> : <span className="monitor-fill-status">不适用</span>}</td><td className="monitor-reason">{monitorReasonLabel(event.reasonCode)}</td><td><button className="monitor-detail-button" type="button" onClick={() => setSelectedEvent(event)}>查看<ChevronRight size={16} /></button></td></tr>)}</tbody></table></div>}
    </Panel>

    <A4SignalLifecyclePanel lifecycles={overview.monitor.signalLifecycles ?? []} counts={overview.monitor.signalLifecycleCounts ?? {}} />

    <LarkNotificationPanel notifications={notifications} />

    <Panel title="A3 计划观察池" icon={<FileClock size={18} />}>
      <dl className="compact-stats monitor-plan-counts"><div><dt>最新 A3 已发布</dt><dd>{latestA3Plans.length}</dd></div><div><dt>今日监测计划</dt><dd>{activePlanCount}</dd></div><div><dt>等待早盘复核</dt><dd>{pendingPlanCount}</dd></div><div><dt>来源运行</dt><dd>最近一次有效研究</dd></div></dl>
      {latestA3Plans.length ? <p className="monitor-plan-roster"><strong>最新 A3 标的</strong>{monitorPlanNames(latestA3Plans)}</p> : null}
      {plans.length === 0 ? <EmptyState title="当前没有待复核或活动计划" detail="A3 只有发布可执行计划后，A4 才会接管对应股票。" icon={<FileClock size={22} />} /> : <div className="data-table-wrap"><table className="data-table monitor-plan-table"><thead><tr><th>股票</th><th>优先级</th><th>状态</th><th>策略</th><th>技术形态</th><th>参考价</th><th>触发区间</th><th>失效价</th><th>压力/减仓</th><th>有效期</th><th>核心依据</th></tr></thead><tbody>{[...plans].sort((left, right) => planPriorityRank(left.planPriority) - planPriorityRank(right.planPriority) || String(left.symbol || "").localeCompare(String(right.symbol || ""))).map((plan) => <tr key={plan.planId || `${plan.laneId}-${plan.symbol}`}><td><span className="monitor-stock"><strong>{plan.name || "名称未提供"}</strong><small>{stockSymbolLabel(plan.symbol)}</small></span></td><td>{planPriorityLabel(plan.planPriority)}</td><td><StatusBadge status={plan.status} /></td><td>{strategyProfileLabel(plan.strategyProfile)}</td><td>{detailValue(plan.setupType)}</td><td className="mono-cell">{detailValue(plan.referencePrice)}</td><td className="mono-cell">{priceRange(plan)}</td><td className="mono-cell">{detailValue(plan.stopLevel)}</td><td className="mono-cell">{detailValue(plan.pressureReducePrice)}</td><td>{formatDateTime(plan.expiresAt)}</td><td className="monitor-plan-reason">{humanizeText(plan.selectionReasons?.[0] || "未提供筛选依据")}</td></tr>)}</tbody></table></div>}
    </Panel>

    <Panel title="A4 回放验收" icon={<ShieldCheck size={18} />} action={replay ? <span className="monitor-test-badge">测试回放 · 非正式信号</span> : undefined}>
      {!replay ? <EmptyState title="尚未生成 A4 回放报告" detail="运行隔离回放后，这里会显示分钟线覆盖、模型调用、有效事件与模拟成交验收结果。" icon={<ShieldCheck size={22} />} /> : <div className="monitor-replay">
        <div className="monitor-replay-head"><div><strong>{replay.testPlan?.name || "名称未提供"}<small>{stockSymbolLabel(replay.testPlan?.symbol)}</small></strong><span>{replay.tradeDate} · {replay.modelMode === "LIVE_DEEPSEEK_FLASH_VETO_ONLY" ? "实时快速模型仅作否决复核" : "确定性放行路径"}</span></div><StatusBadge status={replay.status} /></div>
        <dl className="monitor-replay-metrics"><div><dt>1分钟安全/新鲜度线</dt><dd>{replay.barCoverage?.count ?? "—"} 根</dd></div><div><dt>模型调用</dt><dd>{replay.modelCalls ?? "—"} 次</dd></div><div><dt>有效事件</dt><dd>{replay.effectiveEvents?.length ?? 0} 条</dd></div><div><dt>模拟成交</dt><dd>{replay.fills?.length ?? 0} 笔</dd></div><div><dt>正式 A3 计划</dt><dd>{replay.officialA3PlanCount ?? "—"} 个</dd></div></dl>
        <div className="monitor-replay-contract"><CircleAlert size={17} /><p>该回放把 A3 观察标的临时提升为测试试探计划，只验证盘前复核、触发、模型否决和模拟成交链路；不等于8月28日真实推荐，也不会写入正式状态库。</p></div>
        {replay.effectiveEvents?.length ? <div className="monitor-replay-events">{replay.effectiveEvents.map((event, index) => <button type="button" key={`${event.minuteEnd}-${index}`} onClick={() => setSelectedEvent(event)}><span>{formatDateTime(event.minuteEnd)}</span><strong>{monitorActionLabel(event.action)}</strong><small>{monitorReasonLabel(event.reasonCode)}</small><ChevronRight size={16} /></button>)}</div> : null}
      </div>}
    </Panel>
    <MonitorEventDialog event={selectedEvent} onClose={() => setSelectedEvent(null)} />
  </div>;
}

function a5KindLabel(value?: string | null): string {
  return value === "MIDDAY" ? "盘中复盘" : value === "POST_CLOSE" ? "盘后复盘" : "复盘";
}

function a5VerdictLabel(value?: string | null): string {
  const labels: Record<string, string> = {
    HEALTHY: "运行健康",
    NEEDS_ATTENTION: "需要关注",
    DATA_LIMITED: "数据受限",
    INCIDENT: "存在事故",
    COMPLETED: "已完成",
    READY: "证据就绪",
    DEGRADED: "已完成但证据降级",
    UNAVAILABLE: "证据不可用",
  };
  return labels[String(value || "").toUpperCase()] ?? codeLabel(value) ?? "尚无结论";
}

function a5AttributionLabel(value?: string | null): string {
  const labels: Record<string, string> = {
    GOOD_EXECUTION: "执行符合计划",
    SELECTION_ERROR: "选股问题",
    PLAN_ERROR: "计划问题",
    CONFIRM_ERROR: "择时确认问题",
    DATA_ERROR: "数据问题",
    MARKET_REVERSAL: "盘中市场反转",
    DATA_LIMITED: "数据不足，暂缓归因",
    UNCLASSIFIED: "暂未归类",
    NOT_AN_ERROR: "非错误样本",
  };
  return labels[String(value || "").toUpperCase()] ?? codeLabel(value) ?? "暂未归类";
}

function metricNumber(review: A5DailyReview, key: string): number {
  const value = review.metrics?.[key];
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function A5ReviewPage({ reviews }: { reviews: A5DailyReview[] }) {
  const latest = new Map<string, A5DailyReview>();
  for (const review of reviews) {
    const kind = review.reviewKind || "UNKNOWN";
    if (!latest.has(kind)) latest.set(kind, review);
  }
  const visible = [latest.get("MIDDAY"), latest.get("POST_CLOSE")].filter((item): item is A5DailyReview => Boolean(item));
  return <div className="page-stack">
    <div className="page-heading"><p className="eyebrow">A2 → A3 → A4 独立验证闭环</p><h1>A5 复盘评估</h1><p>每日盘中与盘后各复盘一次。A5 使用异源行情反查板块、技术与信号，并追踪当日强势却未被捕获的反例；它不参与交易，也不会自动修改生产策略。</p></div>
    {visible.length === 0 ? <Panel title="每日复盘" icon={<Gauge size={18} />}><EmptyState title="尚无 A5 复盘" detail="交易日 11:35 生成盘中复盘，16:00 生成盘后复盘；分别使用截至 11:30 和 15:00 的闭合事实。" icon={<Gauge size={22} />} /></Panel> : visible.map((review) => <A5ReviewCard key={review.reviewId || `${review.tradeDate}-${review.reviewKind}`} review={review} />)}
  </div>;
}

function A5ReviewCard({ review }: { review: A5DailyReview }) {
  const report = review.report ?? {};
  const layers: Array<[string, typeof report.a2_review]> = [
    ["A2 题材与选股", report.a2_review], ["A3 日线计划", report.a3_review], ["A4 日内择时", report.a4_review],
  ];
  return <Panel title={`${a5KindLabel(review.reviewKind)}｜${review.tradeDate || "日期未提供"}`} icon={<Gauge size={18} />} action={<StatusBadge status={review.status} />} className="a5-review-card">
    <div className="a5-review-hero"><div><span>总体评价</span><strong>{a5VerdictLabel(report.overall_verdict)}</strong><p>{humanizeText(report.executive_summary || "复盘摘要未提供。")}</p></div><dl><div><dt>事实截止</dt><dd>{formatDateTime(review.cutoffAt)}</dd></div><div><dt>分析模型</dt><dd>{modelNameLabel(review.model)}</dd></div><div><dt>证据质量</dt><dd>{a5VerdictLabel(review.dataQuality?.status)}</dd></div><div><dt>异源核验</dt><dd>{a5VerdictLabel(String(review.metrics?.a5_independent_verification_status || "UNAVAILABLE"))}</dd></div></dl></div>
    <div className="a5-metrics"><div><span>A2 聚焦 / 观察</span><strong>{metricNumber(review, "a2_focus_count")} / {metricNumber(review, "a2_watch_count")}</strong></div><div><span>A2 主题</span><strong>{metricNumber(review, "a2_theme_count")}</strong></div><div><span>A3 计划</span><strong>{metricNumber(review, "a3_plan_count")}</strong></div><div><span>A4 有效事件 / 生命周期</span><strong>{metricNumber(review, "a4_effective_event_count")} / {metricNumber(review, "a4_lifecycle_count")}</strong></div></div>
    <div className="a5-layer-grid">{layers.map(([title, layer]) => <section key={title}><header><h3>{title}</h3><span>{a5VerdictLabel(layer?.verdict)}</span></header><p>{humanizeText(layer?.summary || "本次没有足够事实形成评价。")}</p>{layer?.defects?.length ? <ul>{layer.defects.map((item, index) => <li key={`${item}-${index}`}>{humanizeText(item)}</li>)}</ul> : <small>当前未确认层级缺陷。</small>}</section>)}</div>
    {report.signal_reviews?.length ? <section className="a5-section"><header><h3>信号逐项评价</h3><span>{report.signal_reviews.length} 条</span></header><div className="data-table-wrap"><table className="data-table"><thead><tr><th>股票</th><th>策略</th><th>生命周期</th><th>归因</th><th>客观评价</th></tr></thead><tbody>{report.signal_reviews.map((item, index) => <tr key={`${item.symbol}-${index}`}><td><span className="monitor-stock"><strong>{item.name || "名称未提供"}</strong><small>{stockSymbolLabel(item.symbol)}</small></span></td><td>{strategyProfileLabel(item.strategy_profile)}</td><td>{codeLabel(item.lifecycle_status)}</td><td>{a5AttributionLabel(item.attribution)}</td><td>{humanizeText(item.assessment || "—")}</td></tr>)}</tbody></table></div></section> : null}
    {report.missed_opportunity_reviews?.length ? <section className="a5-section a5-counterexamples"><header><h3>反向拷问：系统漏掉的机会</h3><span>{report.missed_opportunity_reviews.length} 个反例</span></header><p className="a5-section-note">按异源行情找出当日表现较强但没有有效盘中事件的股票，再定位漏在题材筛选、日线计划还是盘中确认。上涨本身不等于当时存在合规买点。</p><div className="data-table-wrap"><table className="data-table"><thead><tr><th>股票</th><th>主题</th><th>客观表现</th><th>漏斗位置</th><th>复盘判断</th></tr></thead><tbody>{report.missed_opportunity_reviews.map((item, index) => <tr key={`${item.symbol}-${index}`}><td><span className="monitor-stock"><strong>{item.name || "名称未提供"}</strong><small>{stockSymbolLabel(item.symbol)}</small></span></td><td>{humanizeText(item.theme || "未映射")}</td><td>{humanizeText(item.observed_performance || "—")}</td><td><span className={item.is_confirmed_defect ? "a5-confirmed-defect" : "a5-shadow-label"}>{codeLabel(item.funnel_drop_stage)} · {item.is_confirmed_defect ? "已确认缺陷" : "待验证"}</span></td><td>{humanizeText(item.assessment || "—")}</td></tr>)}</tbody></table></div></section> : null}
    <div className="a5-bottom-grid"><section className="a5-section"><header><h3>核心缺陷</h3><span>按证据分级</span></header>{report.core_defects?.length ? <ol>{report.core_defects.map((item, index) => <li key={`${item.layer}-${index}`}><strong>{item.layer} · {codeLabel(item.severity)}</strong><p>{humanizeText(item.problem)}</p></li>)}</ol> : <p className="a5-empty-copy">本次未确认可归因的核心缺陷。</p>}</section><section className="a5-section"><header><h3>改进提案</h3><span>仅影子验证</span></header>{report.improvement_proposals?.length ? <ol>{report.improvement_proposals.map((item, index) => <li key={item.proposal_id || index}><strong>{item.target} · {codeLabel(item.type)}</strong><p>{humanizeText(item.proposed_change)}</p><small>至少观察 {item.min_shadow_days ?? "—"} 个交易日；证伪：{humanizeText(item.falsification_criteria || "未提供")}</small></li>)}</ol> : <p className="a5-empty-copy">本次没有达到提出改进实验所需的证据门槛。</p>}</section></div>
    <section className="a5-unresolved"><strong>尚不能下结论</strong><ul>{(report.unresolved_questions ?? []).map((item, index) => <li key={`${item.question}-${index}`}>{humanizeText(item.question)}：{humanizeText(item.resolution)}</li>)}</ul></section>
  </Panel>;
}

function AccountsPage({ overview }: { overview: OverviewResponse }) {
  return <div className="page-stack"><PageHeading eyebrow="本地隔离模拟" title="模拟账户" detail="三个研究模型使用隔离账户；页面只展示本地模拟状态。" />
    {overview.accounts.length === 0 ? <Panel title="账户状态" icon={<WalletCards size={18} />}><EmptyState title="暂无模拟账户数据" detail="研究引擎返回账户后会自动显示。" /></Panel> : <div className="account-list">{overview.accounts.map((account) => <AccountPanel key={account.accountId} account={account} />)}</div>}
    <Panel title="计划状态" icon={<Gauge size={18} />}><dl className="plan-counts">{Object.entries(overview.planCounts).map(([status, count]) => <div key={status}><dt>{statusLabel(status)}</dt><dd>{count}</dd></div>)}</dl>{Object.keys(overview.planCounts).length === 0 ? <EmptyState title="暂无计划计数" detail="尚未创建有效 A3 计划。" /> : null}</Panel>
  </div>;
}

function AccountPanel({ account }: { account: AccountSummary }) {
  const lane = Object.entries(MODEL_LABELS).find(([key]) => account.accountId.includes(key));
  return <section className="account-row"><div className="account-identity"><div className="account-icon"><WalletCards size={19} /></div><div><strong>{lane?.[1] ?? modelNameLabel(account.model)}</strong><span>独立模拟账户</span></div></div><StatusBadge status={account.status} /><div><span>现金</span><strong>{formatMoney(account.cash)}</strong></div><div><span>权益</span><strong>{formatMoney(account.equity)}</strong></div><div><span>持仓</span><strong>{account.positions ?? 0}</strong></div></section>;
}

function LogsPage({ logs, streamConnected }: { logs: LogEntry[]; streamConnected: boolean }) {
  const [level, setLevel] = useState("all");
  const [query, setQuery] = useState("");
  const filtered = useMemo(() => logs.filter((entry) => (level === "all" || entry.level.toLowerCase() === level) && (!query || `${entry.job ?? ""} ${entry.message}`.toLowerCase().includes(query.toLowerCase()))), [logs, level, query]);
  return <div className="page-stack"><PageHeading eyebrow="脱敏运行记录" title="运行日志" detail="汇总控制台调度和研究引擎输出；密钥、认证信息和模型思考正文不会展示。" />
    <Panel title="日志流" icon={<ScrollText size={18} />} action={<span className={`live-indicator ${streamConnected ? "stream-online" : "stream-offline"}`}><span />{streamConnected ? "实时连接" : "等待重连"}</span>}>
      <div className="log-toolbar"><label><Filter size={16} /><span className="sr-only">日志级别</span><select value={level} onChange={(event) => setLevel(event.target.value)}><option value="all">全部级别</option><option value="info">信息</option><option value="warn">警告</option><option value="error">错误</option></select></label><label className="search-field"><span className="sr-only">搜索日志</span><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="搜索任务或消息" /></label><span>{filtered.length} 条</span></div>
      <LogTable logs={filtered} />
    </Panel></div>;
}

function DeploymentPage({ overview }: { overview: OverviewResponse }) {
  const service = overview.service;
  const a1 = overview.a1Generation;
  return <div className="page-stack"><PageHeading eyebrow="运行控制台" title="部署状态" detail="用于核对控制台服务、研究状态库和定时计划，不在页面执行部署操作。" />
    <div className="deployment-grid"><Panel title="服务状态" icon={<Server size={18} />}><dl className="definition-list"><Definition label="控制台服务" value={<StatusBadge status={service.status} />} /><Definition label="运行时长" value={formatDuration((service.uptimeSeconds ?? 0) * 1000)} /><Definition label="时区" value={service.timezone === "Asia/Shanghai" ? "北京时间" : humanizeText(service.timezone)} /><Definition label="监听地址" value={service.host ?? "仅本机"} /><Definition label="版本" value={service.version ?? "—"} /></dl></Panel>
    <Panel title="工作流门禁" icon={<ShieldCheck size={18} />}><dl className="definition-list"><Definition label="状态库" value={<StatusBadge status={service.stateHealthy ? "HEALTHY" : service.stateHealthy === false ? "ERROR" : "UNKNOWN"} />} /><Definition label="配置就绪" value={<StatusBadge status={service.configurationReady ? "READY" : service.configurationReady === false ? "BLOCKED" : "UNKNOWN"} />} /><Definition label="部署门禁" value={<StatusBadge status={service.deploymentReady ? "READY" : service.deploymentReady === false ? "BLOCKED" : "UNKNOWN"} />} /></dl></Panel></div>
    <Panel title="调度计划" icon={<CalendarClock size={18} />}>{overview.schedule.length === 0 ? <EmptyState title="调度信息不可用" detail="任务调度器启动后会报告研究、盯盘与特征维护计划。" /> : <div className="schedule-list">{overview.schedule.map((item, index) => <div key={item.id ?? `${item.label}-${index}`}><div className="schedule-icon"><CalendarClock size={17} /></div><div><strong>{humanizeText(item.label)}</strong><span>{item.cron ?? item.time ?? "—"}</span></div><StatusBadge status={item.status ?? "ACTIVE"} /><time>{item.nextRunAt ? `下次 ${formatDateTime(item.nextRunAt)}` : "由交易日历复核"}</time></div>)}</div>}</Panel>
    <Panel title="A1 活跃研究池" icon={<Database size={18} />}><dl className="definition-list"><Definition label="状态" value={<StatusBadge status={a1?.status ?? "MISSING"} />} /><Definition label="维护模式" value={codeLabel(a1?.mode)} /><Definition label="数据时点" value={a1?.as_of ? formatDateTime(a1.as_of) : "尚未完成首次全量"} /><Definition label="研究版本" value={a1?.generation_id ? "当前有效版本" : "—"} /></dl></Panel>
    {service.blockers?.length ? <Panel title="当前阻断项" icon={<TriangleAlert size={18} />}><ul className="blocker-list">{service.blockers.map((blocker) => <li key={blocker}>{humanizeText(blocker)}</li>)}</ul></Panel> : null}
  </div>;
}

function Definition({ label, value }: { label: string; value: ReactNode }) { return <div><dt>{label}</dt><dd>{value}</dd></div>; }
function SummaryItem({ label, value }: { label: string; value: string }) { return <div><span>{label}</span><strong>{value}</strong></div>; }
function PageHeading({ eyebrow, title, detail }: { eyebrow: string; title: string; detail: string }) { return <header className="page-heading"><p className="eyebrow">{eyebrow}</p><h1>{title}</h1><p>{detail}</p></header>; }
