import { readFile, mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { expect, test } from "vitest";
import { a3Display, A3_REASON_LABELS } from "../../shared/a3-display.js";
import { ProjectFiles } from "../../server/files.js";
import { loadConfig } from "../../server/config.js";
import { LogStore } from "../../server/logger.js";
import { humanizeText } from "../../web/src/localization.js";

const fixture = JSON.parse(await readFile("test/fixtures/a3-20260907-display.json", "utf8")) as {
  rows: { pool: string; item: Record<string, unknown> }[];
};

test("frozen production 49 rows preserve 26/16/5/2 without mutating execution fields", () => {
  const before = JSON.stringify(fixture);
  const counts: Record<string, number> = {};
  for (const row of fixture.rows) {
    const d = a3Display(row.item, row.pool);
    counts[d.disposition] = (counts[d.disposition] ?? 0) + 1;
    expect(d.strategy).toBeTruthy();
    expect(d.untranslatedCodes).toEqual([]);
    if (row.pool === "rejected") {
      expect(d.untranslatedCodes).toEqual([]);
      expect(d.primaryReason).not.toContain("盘中确认");
      expect(d.primaryReason).not.toContain("背景偏弱");
    } else {
      expect(d.blockers).toEqual([]);
    }
  }
  expect(counts).toEqual({ QUALIFIED: 26, WATCH: 16, REJECTED: 5, DATA_GAP: 2 });
  expect(JSON.stringify(fixture)).toBe(before);
});

test("unknown fields are not qualified; background-only is not a rejection explanation", () => {
  const d = a3Display({ reason_codes: ["HIGHER_TIMEFRAME_CONDITIONAL_PROBE"] }, "rejected");
  expect(d.disposition).toBe("UNKNOWN");
  expect(d.primaryReason).toContain("缺少可核验");
  expect(d.blockers).toEqual([]);
  expect(a3Display({ eligibility: "WATCH", reason_codes: ["NEW_UNKNOWN_GATE"] }, "rejected").untranslatedCodes).toEqual(["NEW_UNKNOWN_GATE"]);
});

test("technical qualification cannot hide downstream nonpromotion; watch is readable", () => {
  expect(a3Display({ eligibility: "QUALIFIED" }, "rejected").disposition).toBe("UNKNOWN");
  expect(humanizeText("WATCH")).toBe("技术待观察");
  expect(a3Display({ local_decision: true, sent_to_llm: true }, "approved").localDecision).toBe(false);
  const display = a3Display({ eligibility: "QUALIFIED", reason_codes: ["A3_STOP_DISTANCE_OUTSIDE_LIMIT"] }, "approved");
  expect(display.blockers).toEqual([]);
  expect(display.researchNotes).toEqual(["研究止损距离超出参考区间"]);
});

test("price explanation uses only supplied numeric daily evidence", () => {
  const row = fixture.rows.find(x => x.item.symbol === "000829.SZ")!;
  const display = a3Display({ ...row.item, deterministic_daily_ma: { ma5: 9.102 } }, "rejected");
  expect(display.primaryReason).toContain("9.05");
  expect(display.primaryReason).toContain("9.102");
  expect(a3Display(row.item, "rejected").primaryReason).not.toContain("9.102");
});

test("independent veto remains visible when data is missing", () => {
  const row = fixture.rows.find(x => x.item.symbol === "688001.SH")!;
  const display = a3Display(row.item, "rejected");
  expect(display.disposition).toBe("DATA_GAP");
  expect(display.primaryReason).toBe(A3_REASON_LABELS.HIGHER_TIMEFRAME_BEARISH);
  expect(display.blockers.some(x => x.includes("题材"))).toBe(true);
});

for (const indexed of [false, true]) test(`stage detail filters before pagination, index=${indexed}`, async () => {
  const root = await mkdtemp(join(tmpdir(), "a3-display-"));
  try {
    const output: Record<string, unknown[]> = { core_watch_pool: [], secondary_watch_pool: [], rejected_candidates: [] };
    const pools: Record<string, string> = { approved: "core_watch_pool", watch: "secondary_watch_pool", rejected: "rejected_candidates" };
    for (const row of fixture.rows) output[pools[row.pool]!]!.push(row.item);
    const dir = join(root, "outputs/research");
    await mkdir(dir, { recursive: true });
    const stage = { stage: "A3", status: "VALIDATED", symbols: fixture.rows.filter(x => x.pool === "approved").map(x => x.item.symbol), output };
    await writeFile(join(dir, "research_fixture_lane_1.json"), JSON.stringify({ lane: "lane_1", stages: [stage] }));
    if (indexed) {
      await writeFile(join(dir, "research_fixture_lane_1.decisions.json"), JSON.stringify({ schema_version: "research-stage-decision-index/1.0.0", run_id: "fixture", lane_id: "lane_1", data_file: "research_fixture_lane_1.decisions.ndjson", counts: { A3: { approved: 26, watch: 0, rejected: 23 } } }));
      await writeFile(join(dir, "research_fixture_lane_1.decisions.ndjson"), fixture.rows.map(row => JSON.stringify({ stage: "A3", ...row })).join("\n"));
    }
    const config = loadConfig({}, root);
    const files = new ProjectFiles(config, new LogStore(config));
    const detail = await files.researchStageDetail("fixture", "lane_1", "A3", "rejected", 2, 5, "", "", "WATCH");
    expect(detail?.total).toBe(16);
    expect(detail?.items).toHaveLength(5);
    expect(detail?.dispositionCounts).toEqual({ WATCH: 16, REJECTED: 5, DATA_GAP: 2 });
    for (const row of detail!.items) {
      expect(row.pool).toBe("rejected"); // legacy identity stays intact
      expect(row.status).toBe("REJECTED");
      expect(row.plan?.eligibility).toBe("WATCH");
      expect(row.a3Display?.label).toBe("技术待观察");
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
