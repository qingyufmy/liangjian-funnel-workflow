import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { normalizeWorkflowTiming } from "../../server/files.js";
import { WorkflowTimingTable } from "../../web/src/WorkflowTimingTable";

describe("WP5 visible stage timing component", () => {
  it("renders DTO values, parent denominator, repeated visits, and concurrent-time boundary", () => {
    const timing = normalizeWorkflowTiming({ schema_version: "workflow-timing/1.0",
      python_elapsed_seconds: 10, run_wall_elapsed_seconds: 100, parent_elapsed_seconds: 30,
      budget_seconds: 100, budget_source: "NODE_TIMEOUT_FOR_JOB",
      totals: [{ kind: "PHASE", phase: "DATA_SYNC", elapsed_seconds: 10, current_invocation_elapsed_seconds: 10, visits_count: 2 }],
      visits: [{ kind: "RESEARCH_STAGE", lane_id: "LANE_1", phase: "A2", elapsed_seconds: 10, status: "RUNNING" }],
    });
    const html = renderToStaticMarkup(createElement(WorkflowTimingTable, { timing, stale: true }));
    expect(html).toContain("父任务预算");
    expect(html).toContain("30.0%");
    expect(html).toContain("10.0%");
    expect(html).toContain("数据同步");
    expect(html).toContain("<td>2</td>");
    expect(html).toContain("不可加总为总墙钟");
    expect(html).toContain("失联前记录，未确认结束");
    expect(html).not.toContain("阶段完成");
  });
  it("renders legacy budget as unknown, never as 90 minutes", () => {
    const html = renderToStaticMarkup(createElement(WorkflowTimingTable));
    expect(html).toContain("预算分母未知");
    expect(html).not.toContain("90");
  });
  it("is mounted on the actual overview progress panel, not an isolated preview", () => {
    const app = readFileSync("web/src/App.tsx", "utf8");
    expect(app).toContain('<WorkflowTimingTable timing={progress.timing} stale={progress.stale} />');
    expect(app).toContain('<WorkflowProgressPanel progress={overview.workflowProgress} />');
  });
});
