import { mkdtemp, readdir, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { loadConfig } from "../../server/config.js";
import { LarkSettingsStore } from "../../server/lark-settings.js";
import { LogStore } from "../../server/logger.js";
import { redactText, sanitizeJson } from "../../server/redaction.js";
import { redactTeamUiStderr } from "../../server/team-ui-error-evidence.js";

const fakePath = "/open-apis/bot/v2/hook/FAKE_NOT_A_CREDENTIAL";

test.each(["open.feishu.cn", "open.larksuite.com", "OPEN.FEISHU.CN"])("save and reload %s without exposing destination", async (host) => {
  const root = await mkdtemp(join(tmpdir(), "funnel-feishu-"));
  const store = new LarkSettingsStore(root);
  const status = await store.save(`https://${host}${fakePath}`);
  expect(status.configured).toBe(true);
  expect(status.masked).toBe("••••TIAL");
  expect(await new LarkSettingsStore(root).status()).toEqual(status);
  expect(JSON.stringify(status)).not.toContain(host);
});

test.each([
  `http://open.feishu.cn${fakePath}`, `https://open.feishu.cn:444${fakePath}`,
  `https://user:pass@open.feishu.cn${fakePath}`, `https://open.feishu.cn${fakePath}?q=x`,
  `https://open.feishu.cn${fakePath}#x`, `https://open.feishu.cn.evil.test${fakePath}`,
  `https://evil.test${fakePath}`, "https://open.feishu.cn/wrong/FAKE_TOKEN",
  "https://open.feishu.cn/open-apis/bot/v2/hook/short",
  "https://open.feishu.cn/open-apis/bot/v2/hook/FAKE%2F_TOKEN",
])("rejects invalid fictional destination %s", async (url) => {
  const root = await mkdtemp(join(tmpdir(), "funnel-feishu-invalid-"));
  await expect(new LarkSettingsStore(root).save(url)).rejects.toThrow("Webhook 格式无效");
});

test.each(["open.feishu.cn", "open.larksuite.com", "OPEN.FEISHU.CN"])("redacts %s in log storage, readback and evidence", async (host) => {
  const url = `https://${host}${fakePath}`;
  expect(redactText(url)).toBe("[REDACTED_WEBHOOK]");
  expect(sanitizeJson({ nested: [url] })).toEqual({ nested: ["[REDACTED_WEBHOOK]"] });
  expect(redactTeamUiStderr(url)).toBe("[REDACTED_URL]");
  const root = await mkdtemp(join(tmpdir(), "funnel-feishu-log-"));
  const config = loadConfig({}, root);
  const logs = new LogStore(config);
  expect(logs.error(`failed ${url}`).message).toBe("failed [REDACTED_WEBHOOK]");
  await expect.poll(async () => {
    try {
      const names = await readdir(config.nodeLogDir);
      return await readFile(join(config.nodeLogDir, names[0]!), "utf8");
    } catch { return ""; }
  }).toContain("[REDACTED_WEBHOOK]");
  const names = await readdir(config.nodeLogDir);
  const persisted = await readFile(join(config.nodeLogDir, names[0]!), "utf8");
  expect(persisted).not.toContain("FAKE_NOT_A_CREDENTIAL");
  expect(JSON.stringify(await new LogStore(config).list())).not.toContain(url);
});
