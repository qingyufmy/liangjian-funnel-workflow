import type { JsonRecord, ResearchStageDetailItem } from "./types.js";

const obj = (value: unknown): JsonRecord => value && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : {};
const text = (value: unknown): string | null => typeof value === "string" && value ? value : null;

/** Match the publication ledger, never the research row's proposed expiry. */
export function publicationFor(item: ResearchStageDetailItem, runId: string, laneId: string, records: readonly unknown[], now = Date.now()) {
  const matches = records.map(obj).filter((row) => text(row.source_run_id ?? row.sourceRunId) === runId
    && text(row.lane_id ?? row.laneId) === laneId && row.symbol === item.symbol
    && (!item.plan?.planId || text(row.plan_id ?? row.planId) === item.plan.planId)
    && (!item.plan?.strategyProfile || text(row.strategy_profile ?? row.strategyProfile) === item.plan.strategyProfile));
  const unique = [...new Map(matches.filter((row) => text(row.plan_id ?? row.planId))
    .map((row) => [text(row.plan_id ?? row.planId), row])).values()];
  if (unique.length !== 1) return { state: unique.length > 1 ? "AMBIGUOUS" : "UNVERIFIED", records: unique.length };
  const row = unique[0]!;
  const expiresAt = text(row.expires_at ?? row.expiresAt);
  const status = text(row.status);
  return { state: "MATCHED", records: 1, planId: text(row.plan_id ?? row.planId),
    sourceRunId: runId, laneId, status,
    effectiveStatus: expiresAt && Number.isFinite(Date.parse(expiresAt)) && Date.parse(expiresAt) <= now
      && ["ACTIVE_TODAY", "PENDING_MORNING_REVIEW", "DRAFT_CLOSE"].includes(status ?? "") ? "EXPIRED" : status,
    validFrom: text(row.valid_from ?? row.validFrom), expiresAt,
    targetTradeDate: text(row.target_trade_date ?? row.targetTradeDate) ?? (expiresAt ? shanghaiDate(expiresAt) : null) };
}

export function shanghaiDate(value: string | number): string | null {
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit" }).format(date) : null;
}

export function dailyReviewHealth(reviews: readonly unknown[], notifications: readonly unknown[], now = Date.now()) {
  const today = shanghaiDate(now);
  const current = reviews.map(obj).filter((r) => r.tradeDate === today || r.trade_date === today)
    .sort((a, b) => String(b.cutoffAt ?? b.cutoff_at ?? "").localeCompare(String(a.cutoffAt ?? a.cutoff_at ?? "")));
  const review = current[0];
  const id = review ? text(review.reviewId ?? review.review_id) : null;
  const deliveries = id ? notifications.map(obj).filter((n) => (n.sourceId ?? n.source_id) === id) : [];
  const delivery = deliveries.sort((a, b) => String(b.updatedAt ?? b.updated_at ?? "").localeCompare(String(a.updatedAt ?? a.updated_at ?? "")))[0];
  return { date: today, reviewId: id, reviewStatus: text(review?.status),
    cutoffAt: text(review?.cutoffAt ?? review?.cutoff_at),
    deliveryStatus: text(delivery?.status), sentAt: text(delivery?.sentAt ?? delivery?.sent_at) };
}
