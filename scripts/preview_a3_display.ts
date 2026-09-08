/** Local-only real component/API preview over a compact frozen fixture. */
import express from "express";
import { createServer } from "vite";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, basename } from "node:path";
import { ProjectFiles } from "../server/files.js";
import { loadConfig } from "../server/config.js";
import { LogStore } from "../server/logger.js";

const root = await mkdtemp(join(tmpdir(), "a3-preview-"));
const fixture = JSON.parse(await readFile("test/fixtures/a3-20260907-display.json", "utf8"));
const output: Record<string, unknown[]> = { core_watch_pool: [], secondary_watch_pool: [], rejected_candidates: [] };
const poolNames: Record<string, string> = { approved: "core_watch_pool", watch: "secondary_watch_pool", rejected: "rejected_candidates" };
for (const row of fixture.rows) output[poolNames[row.pool]!]!.push(row.item);
await mkdir(join(root, "outputs/research"), { recursive: true });
await writeFile(join(root, "outputs/research/research_fixture_lane_1.json"), JSON.stringify({ lane: "lane_1", stages: [{ stage: "A3", status: "VALIDATED", symbols: fixture.rows.filter((x: {pool: string}) => x.pool === "approved").map((x: {item: {symbol: string}}) => x.item.symbol), output }] }));
const config = loadConfig({}, root);
const files = new ProjectFiles(config, new LogStore(config));
const app = express();
app.get("/api/research/runs/fixture/lanes/lane_1/stages/A3", async (req, res) => {
  const q = req.query;
  const detail = await files.researchStageDetail("fixture", "lane_1", "A3", String(q.pool ?? "approved"), Number(q.page ?? 1), Number(q.pageSize ?? 50), String(q.q ?? ""), String(q.reason ?? ""), String(q.disposition ?? ""));
  res.json(detail);
});
const vite = await createServer({ configFile: false, server: { middlewareMode: true }, appType: "spa" });
app.use(vite.middlewares);
const server = app.listen(4319, "127.0.0.1", () => console.log("Frozen preview: http://127.0.0.1:4319/test/browser/a3-preview.html"));
async function close() {
  server.close();
  await vite.close();
  if (resolve(root).startsWith(resolve(tmpdir()) + "\\") && basename(root).startsWith("a3-preview-")) await rm(root, { recursive: true, force: true });
  process.exit(0);
}
process.once("SIGINT", close);
process.once("SIGTERM", close);
