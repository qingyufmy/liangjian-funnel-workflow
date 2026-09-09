import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { A5ReviewCard } from "../web/src/App";

describe("A5 signal audit display", () => {
  it("keeps historical missing evidence separate from no signals", () => {
    const html = renderToStaticMarkup(createElement(A5ReviewCard, { review: { report: {} } }));
    expect(html).toContain("不能据此认定当日没有信号");
  });
  it("renders server-owned performance, T+1 boundary and audit evidence", () => {
    const html = renderToStaticMarkup(createElement(A5ReviewCard, { review: { report: {
      signal_stock_reviews: [{ event_id: "event-1", symbol: "002826.SZ", name: "易明医药",
        performance_summary: "较昨收+2.00%；信号后+1.00%", entry_audit_summary: "模拟成交；当日不可卖",
        evidence_id: "A5S:event-1", entry_audit: { minute_snapshot_id: "minute-1", live_reward_risk: 2.7, minimum_reward_risk: 2.5 } }],
    } } }));
    expect(html).toContain("较昨收+2.00%");
    expect(html).toContain("当日不可卖");
    expect(html).toContain("A5S:event-1");
    expect(html).toContain("<summary>");
    expect(html).toContain('scope="col"');
    expect(html).toContain("2.70");
  });
});
