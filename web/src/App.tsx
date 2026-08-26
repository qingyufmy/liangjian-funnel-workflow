import {
  Activity,
  CalendarClock,
  CheckCircle2,
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
  ScrollText,
  Server,
  ShieldCheck,
  TriangleAlert,
  WalletCards,
  X,
  XCircle,
} from "lucide-react";
import { FormEvent, ReactNode, useCallback, useEffect, useMemo, useState } from "react";
import { apiFetch, getStoredToken, saveToken, withQuery } from "./api";
import {
  AccountSummary,
  ApiError,
  DataSourceSummary,
  EffectiveEvent,
  HealthTone,
  LaneSummary,
  LogEntry,
  LogsResponse,
  OverviewResponse,
  RunSummary,
  RunsResponse,
  StageSummary,
  WorkflowProgressLane,
  WorkflowProgressStage,
  WorkflowProgressSummary,
} from "./types";

type ViewId = "overview" | "funnel" | "monitor" | "accounts" | "logs" | "deployment";

const NAVIGATION: Array<{ id: ViewId; label: string; icon: typeof LayoutDashboard }> = [
  { id: "overview", label: "总览", icon: LayoutDashboard },
  { id: "funnel", label: "研究漏斗", icon: GitBranch },
  { id: "monitor", label: "盘中盯盘", icon: MonitorDot },
  { id: "accounts", label: "模拟账户", icon: WalletCards },
  { id: "logs", label: "运行日志", icon: ScrollText },
  { id: "deployment", label: "部署状态", icon: Server },
];

const MODEL_LABELS: Record<string, string> = {
  lane_1: "DeepSeek",
  lane_2: "Kimi",
  lane_3: "GLM",
};

const STAGE_LABELS: Record<string, string> = {
  A1: "A1 基本面",
  A2: "A2 主题情绪",
  A3: "A3 技术计划",
};

const EMPTY_OVERVIEW: OverviewResponse = {
  generatedAt: "",
  service: { status: "unknown" },
  schedule: [],
  latestWorkflow: { status: "unknown", lanes: [] },
  workflowProgress: null,
  monitor: { status: "unknown", events: [] },
  accounts: [],
  planCounts: {},
  dataSources: [],
  recentEffectiveEvents: [],
  recentLogs: [],
};

function normalizeOverview(value: OverviewResponse): OverviewResponse {
  return {
    ...EMPTY_OVERVIEW,
    ...value,
    service: { ...EMPTY_OVERVIEW.service, ...(value.service ?? {}) },
    schedule: Array.isArray(value.schedule) ? value.schedule : [],
    latestWorkflow: {
      ...EMPTY_OVERVIEW.latestWorkflow,
      ...(value.latestWorkflow ?? {}),
      lanes: Array.isArray(value.latestWorkflow?.lanes) ? value.latestWorkflow.lanes : [],
    },
    workflowProgress: value.workflowProgress && typeof value.workflowProgress === "object"
      ? {
        ...value.workflowProgress,
        lanes: Array.isArray(value.workflowProgress.lanes) ? value.workflowProgress.lanes : [],
      }
      : null,
    monitor: {
      ...EMPTY_OVERVIEW.monitor,
      ...(value.monitor ?? {}),
      events: Array.isArray(value.monitor?.events) ? value.monitor.events : [],
    },
    accounts: Array.isArray(value.accounts) ? value.accounts : [],
    planCounts: value.planCounts ?? {},
    dataSources: Array.isArray(value.dataSources) ? value.dataSources : [],
    recentEffectiveEvents: Array.isArray(value.recentEffectiveEvents) ? value.recentEffectiveEvents : [],
    recentLogs: Array.isArray(value.recentLogs) ? value.recentLogs : [],
  };
}

function toneForStatus(status?: string | null): HealthTone {
  const normalized = (status ?? "").toUpperCase();
  if (["OK", "PASS", "HEALTHY", "READY", "READY_TO_PUBLISH", "PUBLISHED", "COMPLETED", "VALIDATED", "ACTIVE"].includes(normalized)) return "healthy";
  if (["RUNNING", "IN_PROGRESS", "STARTED", "RETRYING"].includes(normalized)) return "running";
  if (["WARN", "WARNING", "DEGRADED", "BLOCKED", "PARTIAL", "MISSED"].includes(normalized)) return "warning";
  if (["ERROR", "FAILED", "UNHEALTHY", "STOPPED"].includes(normalized)) return "error";
  return "unknown";
}

function StatusIcon({ tone, size = 16 }: { tone: HealthTone; size?: number }) {
  if (tone === "healthy") return <CheckCircle2 size={size} aria-hidden="true" />;
  if (tone === "running") return <CircleDotDashed size={size} aria-hidden="true" />;
  if (tone === "warning") return <TriangleAlert size={size} aria-hidden="true" />;
  if (tone === "error") return <XCircle size={size} aria-hidden="true" />;
  return <CircleAlert size={size} aria-hidden="true" />;
}

function StatusBadge({ status, label }: { status?: string | null; label?: string }) {
  const tone = toneForStatus(status);
  return (
    <span className={`status-badge status-${tone}`}>
      <StatusIcon tone={tone} size={14} />
      {label ?? statusLabel(status)}
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
    COMPLETED: "已完成",
    VALIDATED: "已验证",
    ACTIVE: "活动",
    RUNNING: "运行中",
    IN_PROGRESS: "进行中",
    DEGRADED: "部分降级",
    BLOCKED: "已阻断",
    FAILED: "执行失败",
    ERROR: "错误",
    STOPPED: "已停止",
    UNKNOWN: "未知",
    NOT_RUN: "尚未运行",
    EMPTY_SCOPE: "无有效范围",
    PASS: "通过",
    DISABLED: "已禁用",
  };
  const key = (status ?? "UNKNOWN").toUpperCase();
  return labels[key] ?? status ?? "未知";
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
  if (milliseconds < 1000) return `${Math.round(milliseconds)}ms`;
  const seconds = Math.floor(milliseconds / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  return `${minutes}m ${rest}s`;
}

function formatMoney(value?: number | null): string {
  if (value === undefined || value === null || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("zh-CN", { style: "currency", currency: "CNY", maximumFractionDigits: 2 }).format(value);
}

function modelLabel(lane: LaneSummary): string {
  return MODEL_LABELS[lane.laneId] ?? lane.model ?? lane.laneId;
}

function nextScheduleLabel(overview: OverviewResponse): string {
  if (!overview.service.schedulerEnabled || !overview.schedule.some((item) => (item.status ?? "").toUpperCase() === "ACTIVE")) return "调度已禁用（验收模式）";
  const explicit = overview.schedule.filter((item) => item.nextRunAt).sort((left, right) => String(left.nextRunAt).localeCompare(String(right.nextRunAt)))[0];
  if (explicit) return `${formatDateTime(explicit.nextRunAt)} ${explicit.label}`;
  const parts = new Intl.DateTimeFormat("en-GB", { timeZone: "Asia/Shanghai", hour: "2-digit", minute: "2-digit", hour12: false }).formatToParts(new Date());
  const hour = Number(parts.find((part) => part.type === "hour")?.value ?? "0");
  const minute = Number(parts.find((part) => part.type === "minute")?.value ?? "0");
  const minuteOfDay = hour * 60 + minute;
  const morning = overview.schedule.find((item) => item.id === "morning");
  const close = overview.schedule.find((item) => item.id === "close");
  if (minuteOfDay < 9 * 60 + 26 && morning) return `09:26 ${morning.label}`;
  if (minuteOfDay < 15 * 60 + 10 && close) return `15:10 ${close.label}`;
  return morning ? `下一工作日 09:26 ${morning.label}` : "等待调度信息";
}

function serviceHeadline(overview: OverviewResponse): { title: string; detail: string; tone: HealthTone } {
  if (overview.activeJob) {
    return { title: `${overview.activeJob.job} 正在运行`, detail: `开始于 ${formatDateTime(overview.activeJob.startedAt)}`, tone: "running" };
  }
  if (overview.service.schedulerEnabled === false) return { title: "服务在线，调度已禁用", detail: "当前为只读验收模式，不会触发研究或盯盘任务", tone: "warning" };
  const tone = toneForStatus(overview.service.status);
  if (tone === "healthy") return { title: "系统运行正常", detail: "Node 服务在线，工作流状态可读取", tone };
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
          <div className="topbar-title"><span>{currentNav?.label}</span><small>Asia/Shanghai</small></div>
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
              {view === "overview" ? <OverviewPage overview={overview} logs={logs} onNavigate={setView} /> : null}
              {view === "funnel" ? <FunnelPage overview={overview} runs={runs} /> : null}
              {view === "monitor" ? <MonitorPage overview={overview} /> : null}
              {view === "accounts" ? <AccountsPage overview={overview} /> : null}
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

function OverviewPage({ overview, logs, onNavigate }: { overview: OverviewResponse; logs: LogEntry[]; onNavigate: (view: ViewId) => void }) {
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

      <LogPanel logs={logs.slice(0, 10)} onOpen={() => onNavigate("logs")} />
    </div>
  );
}

function FunnelPanel({ workflow, onOpen }: { workflow: OverviewResponse["latestWorkflow"]; onOpen: () => void }) {
  const stages = ["A1", "A2", "A3"];
  return (
    <Panel title="研究漏斗" icon={<GitBranch size={18} />} action={<button className="text-button" type="button" onClick={onOpen}>查看运行详情</button>} className="funnel-panel">
      <div className="workflow-meta">
        <div><span>最近运行</span><strong title={workflow.runId ?? undefined}>{workflow.runId ?? "尚未运行"}</strong></div>
        <div><span>时段</span><strong>{workflow.slot ?? "—"}</strong></div>
        <div><span>状态</span><StatusBadge status={workflow.status} /></div>
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
                <th scope="row"><span className="model-name">{modelLabel(lane)}</span><small>{lane.model}</small></th>
                {stages.map((stage) => <StageCell key={stage} stage={lane.stages.find((item) => item.stage.toUpperCase() === stage)} />)}
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}
      <div className="panel-footnote"><Database size={14} /><span>所有数量和状态来自已持久化运行结果；无数据时不会估算进度。</span></div>
    </Panel>
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
    DATA_SYNC: "数据同步",
    SNAPSHOT: "冻结快照",
    SNAPSHOT_RESUMED: "恢复已冻结快照",
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

function progressIssueLabel(issue?: WorkflowProgressSummary["issue"]): string {
  if (issue === "OVERSIZE") return "进度文件超过安全大小限制，已阻断读取";
  if (issue === "INVALID_JSON") return "进度文件格式无效，已停止展示原文";
  if (issue === "INVALID_SHAPE") return "进度文件结构无效，已停止展示原文";
  return "";
}

function progressPercent(processed: number | null, total: number | null): number | null {
  if (processed === null || total === null || total <= 0) return null;
  return Math.max(0, Math.min(100, (processed / total) * 100));
}

function ProgressBar({ processed, total, compact = false }: { processed: number | null; total: number | null; compact?: boolean }) {
  const percent = progressPercent(processed, total);
  return <div className={`progress-bar ${compact ? "progress-bar-compact" : ""}`} role="progressbar" aria-valuemin={0} aria-valuemax={total ?? undefined} aria-valuenow={processed ?? undefined} aria-label="执行进度"><span style={{ width: `${percent ?? 0}%` }} /></div>;
}

function WorkflowProgressPanel({ progress }: { progress: WorkflowProgressSummary | null }) {
  return (
    <Panel title="执行进度" icon={<Activity size={18} />} className="workflow-progress-panel">
      {!progress ? <EmptyState title="暂无持久化进度" detail="首次初始化或研究任务开始后，Python 会将阶段进度写入控制台。" icon={<Activity size={22} />} /> : progress.issue ? (
        <div className="progress-issue" role="status"><StatusIcon tone={progress.status === "BLOCKED" ? "warning" : "error"} size={20} /><div><strong>{progress.status === "BLOCKED" ? "进度读取已阻断" : "进度不可用"}</strong><span>{progressIssueLabel(progress.issue)}</span></div></div>
      ) : (
        <>
          <div className="progress-summary-grid">
            <div><span>当前阶段</span><strong>{progressPhaseLabel(progress.phase)}</strong><StatusBadge status={progress.status} /></div>
            <div><span>总体处理</span><strong>{progressPair(progress.processed, progress.total)}</strong><ProgressBar processed={progress.processed} total={progress.total} /></div>
            <div><span>缓存命中 / 未命中</span><strong>{progressPair(progress.cacheHits, progress.cacheMisses)}</strong></div>
            <div><span>失败数</span><strong className={progress.failures ? "progress-danger" : ""}>{progressCount(progress.failures)}</strong></div>
            <div><span>已用时间</span><strong>{formatDuration(progress.elapsedMs)}</strong></div>
            <div><span>预计剩余</span><strong>{formatDuration(progress.etaMs)}</strong></div>
          </div>
          {progress.lanes.length === 0 ? <div className="progress-empty-lanes">当前阶段尚未产生模型 lane 批次。</div> : <div className="progress-lanes">{progress.lanes.map((lane) => <ProgressLane key={lane.laneId} lane={lane} />)}</div>}
          <div className="panel-footnote"><Activity size={14} /><span>最近更新时间 {formatDateTime(progress.updatedAt)}；此处只显示安全的汇总进度，不展示模型原文。</span></div>
        </>
      )}
    </Panel>
  );
}

function ProgressLane({ lane }: { lane: WorkflowProgressLane }) {
  return <article className="progress-lane"><header><div><strong>{MODEL_LABELS[lane.laneId] ?? lane.laneId}</strong><small>{lane.model ?? "模型未标注"}</small></div><StatusBadge status={lane.status} label={lane.currentStage ? progressPhaseLabel(lane.currentStage) : statusLabel(lane.status)} /></header>
    <div className="progress-lane-meta"><span>批次 {progressPair(lane.batchProcessed, lane.batchTotal)}</span><span>股票 {progressPair(lane.processed, lane.total)}</span></div>
    <ProgressBar processed={lane.processed} total={lane.total} compact />
    {lane.stages.length ? <ul className="progress-stage-list">{lane.stages.map((stage) => <ProgressStage key={stage.stage} stage={stage} />)}</ul> : null}
  </article>;
}

function ProgressStage({ stage }: { stage: WorkflowProgressStage }) {
  return <li><div><strong>{progressPhaseLabel(stage.stage)}</strong><StatusBadge status={stage.status} /></div><span>批次 {progressPair(stage.batchProcessed, stage.batchTotal)} · 股票 {progressPair(stage.processed, stage.total)}</span><ProgressBar processed={stage.processed} total={stage.total} compact /></li>;
}

function StageCell({ stage }: { stage?: StageSummary }) {
  if (!stage) return <td><div className="stage-cell stage-unknown"><StatusBadge status="UNKNOWN" label="无记录" /><small>—</small></div></td>;
  return (
    <td><div className="stage-cell"><StatusBadge status={stage.status} /><small>{stage.symbolCount !== undefined && stage.symbolCount !== null ? `${stage.symbolCount} 只` : "数量未知"}{stage.latencyMs ? ` · ${formatDuration(stage.latencyMs)}` : ""}</small></div></td>
  );
}

function MonitorPanel({ overview, onOpen }: { overview: OverviewResponse; onOpen: () => void }) {
  const effective = overview.recentEffectiveEvents.length ? overview.recentEffectiveEvents : overview.monitor.events.filter((event) => event.effective);
  return (
    <Panel title="盘中盯盘" icon={<MonitorDot size={18} />} action={<button className="text-button" type="button" onClick={onOpen}>查看详情</button>}>
      <div className="rail-summary"><StatusBadge status={overview.monitor.status} /><span>{formatDateTime(overview.monitor.checkedAt)}</span></div>
      {effective.length === 0 ? <EmptyState title="暂无有效事件" detail="A4 会继续复核已有计划；NO_ACTION 不会写入有效结果。" icon={<MonitorDot size={21} />} /> : (
        <ul className="event-list">{effective.slice(0, 4).map((event, index) => <EventRow key={`${event.minuteEnd}-${event.laneId}-${index}`} event={event} />)}</ul>
      )}
      <dl className="compact-stats"><div><dt>有效事件</dt><dd>{overview.monitor.effectiveEventCount ?? effective.length}</dd></div><div><dt>活动计划</dt><dd>{overview.monitor.activePlanCount ?? overview.planCounts.ACTIVE_TODAY ?? 0}</dd></div></dl>
    </Panel>
  );
}

function EventRow({ event }: { event: EffectiveEvent }) {
  return <li><div><strong>{event.action ?? "有效事件"}</strong><span>{event.symbol ?? event.laneId ?? "—"}</span></div><time>{formatDateTime(event.minuteEnd ?? event.time)}</time></li>;
}

function DataSourcesPanel({ sources, onOpen }: { sources: DataSourceSummary[]; onOpen: () => void }) {
  const labels: Record<string, string> = { MODEL_GATEWAY: "模型网关", HITHINK: "同花顺", MOOTDX: "通达信分钟线" };
  return (
    <Panel title="数据源" icon={<Database size={18} />} action={<button className="text-button" type="button" onClick={onOpen}>部署状态</button>}>
      {sources.length === 0 ? <EmptyState title="暂无探针结果" detail="完成能力探针后，这里会显示各事实源的最新状态。" icon={<Database size={21} />} /> : (
        <ul className="source-list">{sources.slice(0, 6).map((source) => (
          <li key={source.id}><span>{labels[source.label.toUpperCase()] ?? source.label}</span><StatusBadge status={source.status} /><time>{formatDateTime(source.checkedAt)}</time></li>
        ))}</ul>
      )}
    </Panel>
  );
}

function LogPanel({ logs, onOpen }: { logs: LogEntry[]; onOpen: () => void }) {
  return (
    <Panel title="实时日志" icon={<ScrollText size={18} />} action={<button className="text-button" type="button" onClick={onOpen}>查看全部日志</button>}>
      <LogTable logs={logs} compact />
    </Panel>
  );
}

function LogTable({ logs, compact = false }: { logs: LogEntry[]; compact?: boolean }) {
  if (!logs.length) return <EmptyState title="暂无 Node 日志" detail="服务启动任务后，脱敏后的运行过程会显示在这里。" icon={<ScrollText size={21} />} />;
  return (
    <div className="log-table-wrap" aria-live="off">
      <table className="log-table"><thead><tr><th>时间</th><th>级别</th><th>任务</th><th>消息</th><th>耗时</th></tr></thead>
        <tbody>{logs.slice(0, compact ? 10 : undefined).map((entry, index) => (
          <tr key={entry.id ?? `${entry.timestamp}-${index}`}>
            <td><time>{formatDateTime(entry.timestamp)}</time></td>
            <td><span className={`log-level level-${entry.level.toLowerCase()}`}>{entry.level.toUpperCase()}</span></td>
            <td>{entry.job ?? "service"}</td><td className="log-message">{entry.message}</td><td>{formatDuration(entry.durationMs)}</td>
          </tr>
        ))}</tbody>
      </table>
    </div>
  );
}

function FunnelPage({ overview, runs }: { overview: OverviewResponse; runs: RunSummary[] }) {
  return (
    <div className="page-stack">
      <PageHeading eyebrow="A1 → A2 → A3" title="研究漏斗" detail="每个模型 lane 严格独立；上游未验证时，下游不会运行。" />
      <WorkflowProgressPanel progress={overview.workflowProgress} />
      <FunnelPanel workflow={overview.latestWorkflow} onOpen={() => undefined} />
      <Panel title="最近运行" icon={<FileClock size={18} />}>
        {runs.length === 0 ? <EmptyState title="暂无运行历史" detail="首次运行完成后，这里会按时间列出研究和盯盘任务。" /> : <RunsTable runs={runs} />}
      </Panel>
    </div>
  );
}

function RunsTable({ runs }: { runs: RunSummary[] }) {
  return <div className="data-table-wrap"><table className="data-table"><thead><tr><th>运行 ID</th><th>任务</th><th>时段</th><th>状态</th><th>完成时间</th><th>耗时</th></tr></thead><tbody>{runs.map((run) => (
    <tr key={run.runId}><td className="mono-cell">{run.runId}</td><td>{run.job ?? "研究"}</td><td>{run.slot ?? "—"}</td><td><StatusBadge status={run.status} /></td><td>{formatDateTime(run.finishedAt ?? run.updatedAt ?? (run.mtimeMs ? new Date(run.mtimeMs).toISOString() : null))}</td><td>{formatDuration(run.durationMs)}</td></tr>
  ))}</tbody></table></div>;
}

function MonitorPage({ overview }: { overview: OverviewResponse }) {
  const events = overview.recentEffectiveEvents.length ? overview.recentEffectiveEvents : overview.monitor.events.filter((event) => event.effective);
  return <div className="page-stack"><PageHeading eyebrow="A4 · veto only" title="盘中盯盘" detail="只复核已有 A3 计划；无权新增候选、放宽触发价或连接真实账户。" />
    <div className="summary-strip"><SummaryItem label="最新检查" value={formatDateTime(overview.monitor.checkedAt)} /><SummaryItem label="有效事件" value={String(overview.monitor.effectiveEventCount ?? events.length)} /><SummaryItem label="活动计划" value={String(overview.monitor.activePlanCount ?? overview.planCounts.ACTIVE_TODAY ?? 0)} /><SummaryItem label="当前状态" value={statusLabel(overview.monitor.status)} /></div>
    <Panel title="有效事件" icon={<MonitorDot size={18} />}>
      {events.length === 0 ? <EmptyState title="目前没有有效盯盘事件" detail="这是合法结果。系统不会把每分钟的 NO_ACTION 写入最终结果。" icon={<MonitorDot size={22} />} /> : <div className="data-table-wrap"><table className="data-table"><thead><tr><th>时间</th><th>Lane</th><th>股票</th><th>动作</th><th>原因</th></tr></thead><tbody>{events.map((event, index) => <tr key={`${event.minuteEnd}-${index}`}><td>{formatDateTime(event.minuteEnd ?? event.time)}</td><td>{event.laneId ?? "—"}</td><td>{event.symbol ?? "—"}</td><td>{event.action ?? "—"}</td><td>{event.reasonCode ?? "—"}</td></tr>)}</tbody></table></div>}
    </Panel></div>;
}

function AccountsPage({ overview }: { overview: OverviewResponse }) {
  return <div className="page-stack"><PageHeading eyebrow="Shadow simulation" title="模拟账户" detail="三个研究模型使用隔离账户；页面只展示本地模拟状态。" />
    {overview.accounts.length === 0 ? <Panel title="账户状态" icon={<WalletCards size={18} />}><EmptyState title="暂无模拟账户数据" detail="Python 状态接口返回账户后会自动显示。" /></Panel> : <div className="account-list">{overview.accounts.map((account) => <AccountPanel key={account.accountId} account={account} />)}</div>}
    <Panel title="计划状态" icon={<Gauge size={18} />}><dl className="plan-counts">{Object.entries(overview.planCounts).map(([status, count]) => <div key={status}><dt>{status}</dt><dd>{count}</dd></div>)}</dl>{Object.keys(overview.planCounts).length === 0 ? <EmptyState title="暂无计划计数" detail="尚未创建有效 A3 计划。" /> : null}</Panel>
  </div>;
}

function AccountPanel({ account }: { account: AccountSummary }) {
  const lane = Object.entries(MODEL_LABELS).find(([key]) => account.accountId.includes(key));
  return <section className="account-row"><div className="account-identity"><div className="account-icon"><WalletCards size={19} /></div><div><strong>{lane?.[1] ?? account.model}</strong><span title={account.accountId}>{account.accountId}</span></div></div><StatusBadge status={account.status} /><div><span>现金</span><strong>{formatMoney(account.cash)}</strong></div><div><span>权益</span><strong>{formatMoney(account.equity)}</strong></div><div><span>持仓</span><strong>{account.positions ?? 0}</strong></div></section>;
}

function LogsPage({ logs, streamConnected }: { logs: LogEntry[]; streamConnected: boolean }) {
  const [level, setLevel] = useState("all");
  const [query, setQuery] = useState("");
  const filtered = useMemo(() => logs.filter((entry) => (level === "all" || entry.level.toLowerCase() === level) && (!query || `${entry.job ?? ""} ${entry.message}`.toLowerCase().includes(query.toLowerCase()))), [logs, level, query]);
  return <div className="page-stack"><PageHeading eyebrow="Sanitized JSONL" title="运行日志" detail="汇总 Node 调度和 Python 子进程输出；密钥、认证头和模型思考正文不会展示。" />
    <Panel title="日志流" icon={<ScrollText size={18} />} action={<span className={`live-indicator ${streamConnected ? "stream-online" : "stream-offline"}`}><span />{streamConnected ? "实时连接" : "等待重连"}</span>}>
      <div className="log-toolbar"><label><Filter size={16} /><span className="sr-only">日志级别</span><select value={level} onChange={(event) => setLevel(event.target.value)}><option value="all">全部级别</option><option value="info">INFO</option><option value="warn">WARN</option><option value="error">ERROR</option></select></label><label className="search-field"><span className="sr-only">搜索日志</span><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="搜索任务或消息" /></label><span>{filtered.length} 条</span></div>
      <LogTable logs={filtered} />
    </Panel></div>;
}

function DeploymentPage({ overview }: { overview: OverviewResponse }) {
  const service = overview.service;
  return <div className="page-stack"><PageHeading eyebrow="Node control plane" title="部署状态" detail="用于核对 Node 服务、Python 状态库和定时计划，不在页面执行部署操作。" />
    <div className="deployment-grid"><Panel title="服务状态" icon={<Server size={18} />}><dl className="definition-list"><Definition label="Node 服务" value={<StatusBadge status={service.status} />} /><Definition label="运行时长" value={formatDuration((service.uptimeSeconds ?? 0) * 1000)} /><Definition label="时区" value={service.timezone ?? "Asia/Shanghai"} /><Definition label="监听地址" value={service.host ?? "仅本机"} /><Definition label="版本" value={service.version ?? "—"} /></dl></Panel>
    <Panel title="工作流门禁" icon={<ShieldCheck size={18} />}><dl className="definition-list"><Definition label="状态库" value={<StatusBadge status={service.stateHealthy ? "HEALTHY" : service.stateHealthy === false ? "ERROR" : "UNKNOWN"} />} /><Definition label="配置就绪" value={<StatusBadge status={service.configurationReady ? "READY" : service.configurationReady === false ? "BLOCKED" : "UNKNOWN"} />} /><Definition label="部署门禁" value={<StatusBadge status={service.deploymentReady ? "READY" : service.deploymentReady === false ? "BLOCKED" : "UNKNOWN"} />} /></dl></Panel></div>
    <Panel title="调度计划" icon={<CalendarClock size={18} />}>{overview.schedule.length === 0 ? <EmptyState title="调度信息不可用" detail="Node 调度器启动后会报告三个固定计划。" /> : <div className="schedule-list">{overview.schedule.map((item, index) => <div key={item.id ?? `${item.label}-${index}`}><div className="schedule-icon"><CalendarClock size={17} /></div><div><strong>{item.label}</strong><span>{item.cron ?? item.time ?? "—"}</span></div><StatusBadge status={item.status ?? "ACTIVE"} /><time>{item.nextRunAt ? `下次 ${formatDateTime(item.nextRunAt)}` : "由交易日历复核"}</time></div>)}</div>}</Panel>
    {service.blockers?.length ? <Panel title="当前阻断项" icon={<TriangleAlert size={18} />}><ul className="blocker-list">{service.blockers.map((blocker) => <li key={blocker}>{blocker}</li>)}</ul></Panel> : null}
  </div>;
}

function Definition({ label, value }: { label: string; value: ReactNode }) { return <div><dt>{label}</dt><dd>{value}</dd></div>; }
function SummaryItem({ label, value }: { label: string; value: string }) { return <div><span>{label}</span><strong>{value}</strong></div>; }
function PageHeading({ eyebrow, title, detail }: { eyebrow: string; title: string; detail: string }) { return <header className="page-heading"><p className="eyebrow">{eyebrow}</p><h1>{title}</h1><p>{detail}</p></header>; }
