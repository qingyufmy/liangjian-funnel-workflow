import { expect, test } from "vitest";
import { publicationFor, dailyReviewHealth, shanghaiDate } from "../../server/console-truth.js";
import type { ResearchStageDetailItem } from "../../server/types.js";

const item = { symbol: "000998.SZ", plan: { strategyProfile: "TREND_MA5", planExpiry: "2026-09-07T15:00:00+08:00" } } as ResearchStageDetailItem;
const record = { source_run_id: "run-a", lane_id: "lane_1", symbol: "000998.SZ", plan_id: "published-1", strategy_profile: "TREND_MA5", status: "PENDING_MORNING_REVIEW", expires_at: "2026-09-08T15:00:00+08:00" };
const now = Date.parse("2026-09-07T22:00:00+08:00");

test("published plan expiry replaces neither the research artifact nor the strategy", () => {
  expect(publicationFor(item, "run-a", "lane_1", [record, record], now)).toMatchObject({ state: "MATCHED", effectiveStatus: "PENDING_MORNING_REVIEW", targetTradeDate: "2026-09-08", expiresAt: record.expires_at });
  expect(item.plan?.planExpiry).toBe("2026-09-07T15:00:00+08:00");
  expect(publicationFor(item, "run-a", "lane_1", [record], now + 86400000).effectiveStatus).toBe("EXPIRED");
});
test("never join another run, lane or strategy, and preserve ambiguity", () => {
  expect(publicationFor(item, "run-b", "lane_1", [record], now).state).toBe("UNVERIFIED");
  expect(publicationFor(item, "run-a", "lane_2", [record], now).state).toBe("UNVERIFIED");
  expect(publicationFor(item, "run-a", "lane_1", [{ ...record, strategy_profile: "MA520_SWING" }], now).state).toBe("UNVERIFIED");
  expect(publicationFor(item, "run-a", "lane_1", [record, { ...record, plan_id: "published-2" }], now).state).toBe("AMBIGUOUS");
});
test("past review or unrelated delivered message never makes today's A5 healthy", () => {
  expect(dailyReviewHealth([{ reviewId: "old", tradeDate: "2026-09-04", status: "COMPLETED" }], [{ sourceId: "old", status: "SENT" }], now)).toMatchObject({ date: "2026-09-07", reviewStatus: null, deliveryStatus: null });
  expect(dailyReviewHealth([{ reviewId: "today", tradeDate: "2026-09-07", status: "DEGRADED" }], [{ sourceId: "a4", status: "SENT" }], now)).toMatchObject({ reviewStatus: "DEGRADED", deliveryStatus: null });
  expect(dailyReviewHealth([{ reviewId: "today", tradeDate: "2026-09-07", status: "COMPLETED" }], [{ sourceId: "today", status: "FAILED" }], now).deliveryStatus).toBe("FAILED");
  expect(shanghaiDate("2026-09-07T17:00:00Z")).toBe("2026-09-08");
});
