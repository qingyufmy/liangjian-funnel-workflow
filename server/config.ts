import { existsSync } from "node:fs";
import { isAbsolute, join, resolve } from "node:path";

import type { JobDefinition } from "./types.js";

export const TIMEZONE = "Asia/Shanghai";

export const JOB_DEFINITIONS: readonly JobDefinition[] = [
  { name: "morning", command: "run-morning", label: "早盘复核", schedule: "09:26" },
  { name: "close", command: "run-close", label: "收盘研究", schedule: "15:10" },
  { name: "monitor", command: "run-monitor", label: "盘中盯盘", schedule: "交易时段每分钟" },
];

export interface AppConfig {
  readonly rootDir: string;
  readonly host: string;
  readonly port: number;
  readonly pythonBin: string;
  readonly webDist: string;
  readonly nodeLogDir: string;
  readonly timezone: string;
  readonly dashboardToken: string | null;
  readonly jobTimeoutMs: number;
  readonly statusTimeoutMs: number;
  readonly statusCacheMs: number;
  readonly maxLogLineLength: number;
  readonly maxMemoryLogs: number;
  readonly schedulerEnabled: boolean;
}

function positiveInteger(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function booleanValue(value: string | undefined, fallback: boolean): boolean {
  if (!value) return fallback;
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function resolvePython(rootDir: string, configured: string | undefined): string {
  const explicit = configured?.trim();
  if (explicit) return explicit;

  const candidates = process.platform === "win32"
    ? [join(rootDir, ".venv", "Scripts", "python.exe")]
    : [join(rootDir, ".venv", "bin", "python")];
  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }
  return "python3";
}

function resolveWebDist(rootDir: string, configured: string | undefined): string {
  const value = configured?.trim() || join(rootDir, "dist", "web");
  return isAbsolute(value) ? resolve(value) : resolve(rootDir, value);
}

export function loadConfig(
  env: NodeJS.ProcessEnv = process.env,
  rootDir: string = process.cwd(),
): AppConfig {
  const resolvedRoot = resolve(rootDir);
  return {
    rootDir: resolvedRoot,
    host: env.HOST?.trim() || env.LIANGJIAN_HOST?.trim() || "127.0.0.1",
    port: positiveInteger(env.PORT?.trim() || env.LIANGJIAN_PORT, 3210),
    pythonBin: resolvePython(resolvedRoot, env.LIANGJIAN_PYTHON_BIN),
    webDist: resolveWebDist(resolvedRoot, env.LIANGJIAN_WEB_DIST),
    nodeLogDir: resolve(resolvedRoot, "outputs", "node"),
    timezone: TIMEZONE,
    dashboardToken: env.LIANGJIAN_DASHBOARD_TOKEN?.trim() || null,
    jobTimeoutMs: positiveInteger(env.LIANGJIAN_JOB_TIMEOUT_MS, 5 * 60 * 60 * 1000),
    statusTimeoutMs: positiveInteger(env.LIANGJIAN_STATUS_TIMEOUT_MS, 20_000),
    statusCacheMs: positiveInteger(env.LIANGJIAN_STATUS_CACHE_MS, 15_000),
    maxLogLineLength: positiveInteger(env.LIANGJIAN_MAX_LOG_LINE_LENGTH, 16_384),
    maxMemoryLogs: positiveInteger(env.LIANGJIAN_MAX_MEMORY_LOGS, 1_000),
    schedulerEnabled: booleanValue(env.LIANGJIAN_SCHEDULER_ENABLED, true),
  };
}
