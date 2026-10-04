import { describe, expect, it } from "vitest";
import { testWorkerOptions } from "./test-workers";

describe("test worker budget", () => {
  it("uses at most two workers when no budget is set", () => {
    expect(testWorkerOptions(undefined)).toEqual({ maxWorkers: 2, minWorkers: 1 });
  });

  it.each([
    ["1", 1],
    ["4", 4],
    [" 2 ", 2],
    ["02", 2],
  ])("uses the selected budget %s without raising the minimum", (input, workers) => {
    expect(testWorkerOptions(input)).toEqual({ maxWorkers: workers, minWorkers: 1 });
  });

  it.each(["", " ", "0", "-1", "+2", "1.5", "50%", "two", "2e1", "Infinity", "9007199254740992"])(
    "rejects an invalid budget: %s",
    (input) => {
      expect(() => testWorkerOptions(input)).toThrow("KINGDOM_TEST_CPUS must be a positive integer.");
    },
  );
});
