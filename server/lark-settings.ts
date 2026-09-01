import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

const LARK_WEBHOOK_PATH = /^\/open-apis\/bot\/v2\/hook\/([A-Za-z0-9_-]{8,200})$/;

export interface LarkSettingsStatus {
  readonly configured: boolean;
  readonly masked: string | null;
  readonly updatedAt: string | null;
}

interface PersistedLarkSettings {
  readonly schemaVersion: 1;
  readonly webhookUrl: string;
  readonly updatedAt: string;
}

export class LarkSettingsValidationError extends Error {
  readonly reasonCode = "LARK_WEBHOOK_INVALID";

  constructor() {
    super("Webhook 格式无效，请填写 Lark 自定义机器人地址");
    this.name = "LarkSettingsValidationError";
  }
}

function validateWebhookUrl(value: unknown): string {
  if (typeof value !== "string" || value.length > 512) throw new LarkSettingsValidationError();
  let parsed: URL;
  try {
    parsed = new URL(value.trim());
  } catch {
    throw new LarkSettingsValidationError();
  }
  const match = LARK_WEBHOOK_PATH.exec(parsed.pathname);
  if (
    parsed.protocol !== "https:"
    || parsed.hostname !== "open.larksuite.com"
    || parsed.port
    || parsed.username
    || parsed.password
    || parsed.search
    || parsed.hash
    || !match
  ) throw new LarkSettingsValidationError();
  return parsed.toString();
}

function safeStatus(value: PersistedLarkSettings | null): LarkSettingsStatus {
  if (!value) return { configured: false, masked: null, updatedAt: null };
  const match = LARK_WEBHOOK_PATH.exec(new URL(value.webhookUrl).pathname);
  const token = match?.[1] ?? "";
  return {
    configured: true,
    masked: `••••${token.slice(-4)}`,
    updatedAt: value.updatedAt,
  };
}

export class LarkSettingsStore {
  readonly path: string;

  constructor(rootDir: string) {
    this.path = join(rootDir, "state", "lark_webhook.json");
  }

  private async readPersisted(): Promise<PersistedLarkSettings | null> {
    try {
      const raw = JSON.parse(await readFile(this.path, "utf8")) as Partial<PersistedLarkSettings>;
      if (raw.schemaVersion !== 1 || typeof raw.updatedAt !== "string") return null;
      const webhookUrl = validateWebhookUrl(raw.webhookUrl);
      return { schemaVersion: 1, webhookUrl, updatedAt: raw.updatedAt };
    } catch {
      return null;
    }
  }

  async status(): Promise<LarkSettingsStatus> {
    return safeStatus(await this.readPersisted());
  }

  async save(webhookUrl: unknown): Promise<LarkSettingsStatus> {
    const normalized = validateWebhookUrl(webhookUrl);
    const updatedAt = new Date().toISOString();
    const payload: PersistedLarkSettings = { schemaVersion: 1, webhookUrl: normalized, updatedAt };
    const parent = dirname(this.path);
    const temporary = `${this.path}.${process.pid}.${Date.now()}.tmp`;
    await mkdir(parent, { recursive: true, mode: 0o700 });
    try {
      await writeFile(temporary, `${JSON.stringify(payload)}\n`, { encoding: "utf8", mode: 0o600, flag: "wx" });
      await rename(temporary, this.path);
      await chmod(this.path, 0o600);
    } catch (error) {
      await rm(temporary, { force: true }).catch(() => undefined);
      throw error;
    }
    return safeStatus(payload);
  }

  async clear(): Promise<LarkSettingsStatus> {
    await rm(this.path, { force: true });
    return safeStatus(null);
  }
}
