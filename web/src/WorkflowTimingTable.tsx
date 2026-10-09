import React from "react";
import type { WorkflowTimingEntry, WorkflowTimingSummary } from "./types";

const labels: Record<string, string> = {
  STARTING: "启动", UNIVERSE_SYNC: "股票范围", DATA_SYNC: "数据同步", MARKET_FACT_SYNC: "行情事实",
  COMPANY_FACT_SYNC: "公司事实", CNINFO_SYNC: "公告检索", CNINFO_PDF_SYNC: "公告原文",
  FACT_MANIFEST_SYNC: "事实清单", OPEN_MACRO_SYNC: "宏观资料", SNAPSHOT: "冻结快照",
  SNAPSHOT_READY: "快照就绪", SNAPSHOT_REUSE: "复用快照", SNAPSHOT_RESUMED: "恢复快照",
  FEATURE_SOURCE_GENERATION: "生成特征来源", EARLY_DISCOVERY_DAILY_SYNC: "早发现日线",
  RESEARCH: "并行研究墙钟", A1: "A1", A2: "A2", A3: "A3", MACRO_DISCOVERY: "宏观发现",
  A1_LOCAL_SCREEN: "A1 本地筛选", A1_LLM_REVIEW: "A1 模型审核", A2_LOCAL_ROLE: "A2 本地角色",
  A2_LLM_REVIEW: "A2 模型审核", A3_LOCAL_TECHNICAL: "A3 本地技术", A3_LLM_REVIEW: "A3 模型审核",
  PERSIST: "结果整理与写入", BOOTSTRAP: "初始化", UNKNOWN: "未识别阶段",
};
function duration(ms: number | null): string {
  if (ms === null || !Number.isFinite(ms)) return "未知";
  return ms < 60_000 ? `${(ms / 1000).toFixed(1)} 秒` : `${(ms / 60_000).toFixed(1)} 分钟`;
}
function label(entry: WorkflowTimingEntry): string {
  const phase = labels[entry.phase] ?? (entry.phase.startsWith("MARKET_FACT_") ? "市场事实子阶段" : "未识别阶段");
  return entry.kind === "RESEARCH_STAGE" ? `${entry.laneId ?? "未识别模型通道"} · ${phase}` : phase;
}
function state(entry: WorkflowTimingEntry, stale: boolean): string {
  if (entry.status === "RUNNING") return stale ? "失联前记录，未确认结束" : "截至最后心跳，未完成";
  if (entry.status === "INTERRUPTED") return "中断，未完成";
  if (entry.status === "FAILED") return "失败";
  if (entry.status === "COMPLETED") return "阶段完成";
  if (entry.status === "LEFT_PHASE" || entry.status === "LEFT_STAGE") return "已离开，结果未确认";
  return "任务已结束，非阶段成功证明";
}

export function WorkflowTimingTable({ timing, stale = false }: { timing?: WorkflowTimingSummary | null; stale?: boolean }) {
  if (!timing) return <div className="progress-no-value">阶段计时尚未提供；预算分母未知。</div>;
  const knownBudget = timing.budgetMs !== null && timing.budgetMs > 0;
  return <section aria-label="阶段耗时与实际预算" className="workflow-timing">
    <div><span>Python 已观测墙钟</span><strong>{duration(timing.pythonElapsedMs)}</strong></div>
    <div><span>研究 run 墙钟（含恢复间隔）</span><strong>{duration(timing.runWallElapsedMs)}</strong></div>
    <div><span>本次父任务预算（原任务超时）</span><strong>{duration(timing.budgetMs)}</strong></div>
    <div><span>父任务已用 / 预算</span><strong>{duration(timing.parentElapsedMs)} / {duration(timing.budgetMs)}{timing.budgetUsedRatio !== null ? ` · ${(timing.budgetUsedRatio * 100).toFixed(1)}%` : " · 占比未知"}</strong></div>
    <p>父任务从启动子进程前计时；Python 墙钟与阶段访问分别记录。并发阶段耗时及预算占比不可加总为总墙钟；恢复空档不计为阶段执行。</p>
    <table><thead><tr><th>累计阶段</th><th>访问次数</th><th>累计耗时</th><th>本次耗时</th><th>本次总预算占比</th></tr></thead><tbody>
      {timing.totals.map((entry, index) => <tr key={index}><td>{label(entry)}</td><td>{entry.visitsCount}</td><td>{duration(entry.elapsedMs)}</td><td>{duration(entry.currentInvocationElapsedMs)}</td><td>{knownBudget && entry.currentInvocationElapsedMs !== null ? `${(entry.currentInvocationElapsedMs / timing.budgetMs! * 100).toFixed(1)}%` : "未知"}</td></tr>)}
    </tbody></table>
    <details><summary>阶段访问记录（{timing.visits.length}）</summary><ul>
      {timing.visits.map((entry, index) => <li key={index}>{label(entry)} · {duration(entry.elapsedMs)} · {state(entry, stale)}</li>)}
    </ul></details>
    {timing.visitsDroppedCount > 0 ? <p>仅保留最近访问明细；另有 {timing.visitsDroppedCount} 条较早记录，累计耗时与次数仍保留。</p> : null}
  </section>;
}
