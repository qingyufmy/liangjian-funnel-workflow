import { createServer, type Server } from "node:http";
import { fileURLToPath, pathToFileURL } from "node:url";

import { createApp } from "./api.js";
import { loadConfig } from "./config.js";
import { DashboardData } from "./dashboard.js";
import { ProjectFiles } from "./files.js";
import { LogStore } from "./logger.js";
import { LarkSettingsStore } from "./lark-settings.js";
import { JobRunner } from "./runner.js";
import { WorkflowScheduler } from "./scheduler.js";

export interface RunningControlPlane {
  readonly server: Server;
  readonly logger: LogStore;
  readonly runner: JobRunner;
  readonly scheduler: WorkflowScheduler;
  readonly stop: () => Promise<void>;
}

export async function startServer(): Promise<RunningControlPlane> {
  const config = loadConfig();
  const startedAt = Date.now();
  const logger = new LogStore(config);
  const runner = new JobRunner(config, logger);
  const scheduler = new WorkflowScheduler(runner, logger, {
    comparisonEnabled: config.comparisonEnabled,
    featureMaintenanceEnabled: config.featureMaintenanceEnabled,
  });
  const files = new ProjectFiles(config, logger);
  const dashboard = new DashboardData(config, files, runner, scheduler, logger);
  const larkSettings = new LarkSettingsStore(config.rootDir);
  const app = createApp({ config, dashboard, runner, scheduler, logger, larkSettings, startedAt });
  const server = createServer(app);
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error): void => {
      server.off("listening", onListening);
      reject(error);
    };
    const onListening = (): void => {
      server.off("error", onError);
      resolve();
    };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(config.port, config.host);
  });
  if (config.schedulerEnabled) scheduler.start();
  else logger.warn("Node 调度器已通过 LIANGJIAN_SCHEDULER_ENABLED=false 禁用");
  logger.info(`控制面已启动 host=${config.host} port=${config.port}`);

  let stopping = false;
  const stop = async (): Promise<void> => {
    if (stopping) return;
    stopping = true;
    scheduler.stop();
    await runner.stop();
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    logger.info("控制面已停止");
  };
  return { server, logger, runner, scheduler, stop };
}

async function main(): Promise<void> {
  const controlPlane = await startServer();
  const shutdown = (): void => {
    void controlPlane.stop().finally(() => process.exit(0));
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

const executedPath = process.argv[1] ? fileURLToPath(pathToFileURL(process.argv[1])) : "";
const currentPath = fileURLToPath(import.meta.url);
if (executedPath && executedPath === currentPath) {
  void main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : "startup failure";
    process.stderr.write(`control-plane startup failed: ${message}\n`);
    process.exitCode = 1;
  });
}
