import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
    // Bound local worker pressure; every file still runs.
    maxWorkers: 2,
    testTimeout: 15_000,
    hookTimeout: 15_000,
  },
});
