import { appendFile, mkdir, readdir, readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import { EventEmitter } from "node:events";

import type { AppConfig } from "./config.js";
import { redactText, asJsonRecord, asString } from "./redaction.js";
import type { JobName, LogEvent } from "./types.js";

type LogLevel = LogEvent["level"];
type LogStream = LogEvent["stream"];
type LogListener = (event: LogEvent) => void;

const ISO_PREFIX = /^(\d{4}-\d{2}-\d{2}T[^\s]+)\s?(.*)$/;
const MAX_HISTORICAL_FILE_BYTES = 2 * 1024 * 1024;

function dateInShanghai(now: Date): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const values = new Map(parts.map((part) => [part.type, part.value]));
  return `${values.get("year") ?? "0000"}-${values.get("month") ?? "00"}-${values.get("day") ?? "00"}`;
}

function parseLevel(value: unknown): LogLevel {
  return value === "debug" || value === "warn" || value === "error" ? value : "info";
}

function parseHistoricalLine(line: string, source: LogStream, id: number): LogEvent {
  const parsed = line.trim();
  if (parsed.startsWith("{")) {
    try {
      const record = asJsonRecord(JSON.parse(parsed));
      if (record) {
        return {
          id,
          timestamp: asString(record.timestamp) ?? new Date().toISOString(),
          level: parseLevel(record.level),
          job: asString(record.job),
          runId: asString(record.runId),
          stream: source,
          message: redactText(asString(record.message) ?? parsed),
        };
      }
    } catch {
      // A partially-written line is rendered as plain text below.
    }
  }
  const match = ISO_PREFIX.exec(parsed);
  return {
    id,
    timestamp: match?.[1] ?? new Date().toISOString(),
    level: source === "stderr" ? "error" : "info",
    job: source === "scheduler" ? "scheduler" : null,
    runId: null,
    stream: source,
    message: redactText(match?.[2] ?? parsed),
  };
}

export class LogStore {
  private readonly events: LogEvent[] = [];
  private readonly emitter = new EventEmitter();
  private sequence = 0;
  private writeChain: Promise<void> = Promise.resolve();

  public constructor(private readonly config: AppConfig) {}

  public subscribe(listener: LogListener): () => void {
    this.emitter.on("log", listener);
    return () => this.emitter.off("log", listener);
  }

  public log(
    level: LogLevel,
    message: string,
    options: { job?: JobName | string | null; runId?: string | null; stream?: LogStream } = {},
  ): LogEvent {
    const event: LogEvent = {
      id: ++this.sequence,
      timestamp: new Date().toISOString(),
      level,
      job: options.job ?? null,
      runId: options.runId ?? null,
      stream: options.stream ?? "node",
      message: redactText(message, this.config.maxLogLineLength),
    };
    this.events.push(event);
    while (this.events.length > this.config.maxMemoryLogs) this.events.shift();
    this.emitter.emit("log", event);
    this.persist(event);
    return event;
  }

  public debug(message: string, options?: { job?: JobName | string | null; runId?: string | null; stream?: LogStream }): LogEvent {
    return this.log("debug", message, options);
  }

  public info(message: string, options?: { job?: JobName | string | null; runId?: string | null; stream?: LogStream }): LogEvent {
    return this.log("info", message, options);
  }

  public warn(message: string, options?: { job?: JobName | string | null; runId?: string | null; stream?: LogStream }): LogEvent {
    return this.log("warn", message, options);
  }

  public error(message: string, options?: { job?: JobName | string | null; runId?: string | null; stream?: LogStream }): LogEvent {
    return this.log("error", message, options);
  }

  public memory(limit = 1000): LogEvent[] {
    return this.events.slice(-limit).reverse();
  }

  public async list(limit = 100, level?: LogLevel, job?: string): Promise<LogEvent[]> {
    const memory = this.events.slice();
    const historical = await this.readHistorical(Math.max(limit * 3, 100));
    const seen = new Set<string>();
    const combined: LogEvent[] = [];
    for (const event of [...memory, ...historical]) {
      const key = `${event.timestamp}|${event.stream}|${event.job ?? ""}|${event.runId ?? ""}|${event.message}`;
      if (seen.has(key)) continue;
      seen.add(key);
      if (level && event.level !== level) continue;
      if (job && event.job !== job) continue;
      combined.push(event);
    }
    combined.sort((left, right) => right.timestamp.localeCompare(left.timestamp) || right.id - left.id);
    return combined.slice(0, limit);
  }

  private persist(event: LogEvent): void {
    const day = dateInShanghai(new Date(event.timestamp));
    const file = join(this.config.nodeLogDir, `node-${day}.jsonl`);
    const line = `${JSON.stringify(event)}\n`;
    this.writeChain = this.writeChain
      .then(async () => {
        await mkdir(this.config.nodeLogDir, { recursive: true });
        await appendFile(file, line, { encoding: "utf8", mode: 0o600 });
      })
      .catch(() => {
        // Logging must never crash the workflow process when the output disk is unavailable.
      });
  }

  private async readHistorical(limit: number): Promise<LogEvent[]> {
    const files: { path: string; source: LogStream; mtime: number }[] = [];
    for (const directory of ["node", "scheduler"]) {
      const folder = join(this.config.rootDir, "outputs", directory);
      try {
        const entries = await readdir(folder, { withFileTypes: true });
        for (const entry of entries) {
          if (!entry.isFile()) continue;
          const matches = directory === "node"
            ? /^node-\d{4}-\d{2}-\d{2}\.jsonl$/.test(entry.name)
            : /^scheduler-\d{4}-\d{2}-\d{2}\.log$/.test(entry.name);
          if (!matches) continue;
          const path = join(folder, entry.name);
          try {
            const metadata = await stat(path);
            if (metadata.size > MAX_HISTORICAL_FILE_BYTES) continue;
            files.push({ path, source: directory === "node" ? "node" : "scheduler", mtime: metadata.mtimeMs });
          } catch {
            // A file can disappear during rotation; skip it safely.
          }
        }
      } catch {
        // The directory is optional on a fresh deployment.
      }
    }
    files.sort((left, right) => right.mtime - left.mtime);
    const output: LogEvent[] = [];
    let id = -1;
    for (const file of files.slice(0, 10)) {
      try {
        const content = await readFile(file.path, { encoding: "utf8" });
        const lines = content.split(/\r?\n/).filter(Boolean).slice(-Math.max(limit, 100));
        for (const line of lines) output.push(parseHistoricalLine(line, file.source, id--));
      } catch {
        // The live logger remains usable even if historical log reading fails.
      }
    }
    return output;
  }
}
