import { describe, expect, it } from "vitest";
import { childEnvironment, timeoutForJob } from "../../server/runner.js";

describe("WP5 parent close deadline", () => {
  it("passes the actual shorter Node budget as an absolute child-only deadline", () => {
    const base = { LIANGJIAN_PARENT_CLOSE_DEADLINE_MS: "old", KEEP: "same" };
    const timeout = timeoutForJob("close", 600_000);
    expect(childEnvironment("close", timeout, 1000, base)).toEqual({
      KEEP: "same", LIANGJIAN_PARENT_CLOSE_DEADLINE_MS: "601000",
      LIANGJIAN_PARENT_JOB_BUDGET_MS: "600000", LIANGJIAN_PARENT_JOB_STARTED_MS: "1000",
    });
    expect(timeoutForJob("close", 10_000_000)).toBe(5_400_000);
    expect(base.LIANGJIAN_PARENT_CLOSE_DEADLINE_MS).toBe("old");
  });
  it("does not pass a stale close deadline to another job", () => {
    expect(childEnvironment("monitor", 55_000, 1000, {
      LIANGJIAN_PARENT_CLOSE_DEADLINE_MS: "old",
    })).toEqual({ LIANGJIAN_PARENT_JOB_BUDGET_MS: "55000", LIANGJIAN_PARENT_JOB_STARTED_MS: "1000" });
  });
  it("injects the actual job denominator, clears stale timing, and does not mutate the server", () => {
    const base = { LIANGJIAN_PARENT_JOB_BUDGET_MS: "stale", LIANGJIAN_PARENT_JOB_STARTED_MS: "stale" };
    expect(childEnvironment("a1", 123000, 1000, base)).toEqual({
      LIANGJIAN_PARENT_JOB_BUDGET_MS: "123000", LIANGJIAN_PARENT_JOB_STARTED_MS: "1000",
    });
    expect(childEnvironment("monitor", null, 1000, base)).toEqual({});
    expect(base.LIANGJIAN_PARENT_JOB_BUDGET_MS).toBe("stale");
  });
  it.each([NaN, Infinity, -1, 0])("rejects invalid budget %s instead of weakening the close deadline", (budget) => {
    expect(() => childEnvironment("close", budget, 1000, { LIANGJIAN_PARENT_CLOSE_DEADLINE_MS: "2000" }))
      .toThrow("INVALID_PARENT_JOB_TIMING");
  });
  it("refuses a missing close budget or overflow instead of clearing the real deadline", () => {
    expect(() => childEnvironment("close", null, 1000)).toThrow("INVALID_PARENT_JOB_TIMING");
    expect(() => childEnvironment("close", Number.MAX_SAFE_INTEGER, 1000)).toThrow("INVALID_PARENT_JOB_TIMING");
  });
});
