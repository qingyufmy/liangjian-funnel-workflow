import { describe, expect, it } from "vitest";
import { childEnvironment, timeoutForJob } from "../../server/runner.js";

describe("WP5 parent close deadline", () => {
  it("passes the actual shorter Node budget as an absolute child-only deadline", () => {
    const base = { LIANGJIAN_PARENT_CLOSE_DEADLINE_MS: "old", KEEP: "same" };
    const timeout = timeoutForJob("close", 600_000);
    expect(childEnvironment("close", timeout, 1000, base)).toEqual({
      KEEP: "same", LIANGJIAN_PARENT_CLOSE_DEADLINE_MS: "601000",
    });
    expect(timeoutForJob("close", 10_000_000)).toBe(5_400_000);
    expect(base.LIANGJIAN_PARENT_CLOSE_DEADLINE_MS).toBe("old");
  });
  it("does not pass a stale close deadline to another job", () => {
    expect(childEnvironment("monitor", 55_000, 1000, {
      LIANGJIAN_PARENT_CLOSE_DEADLINE_MS: "old",
    })).toEqual({});
  });
});
