import { describe, expect, it } from "vitest";
import { plannerKeys } from "./planner-query-keys";

describe("plannerKeys pending sentinels", () => {
  it("isolates search cursors while preserving a shared list invalidation prefix", () => {
    expect(plannerKeys.list(100, " foo ")).toEqual(plannerKeys.list(100, "foo"));
    expect(plannerKeys.list(100, "foo")).not.toEqual(plannerKeys.list(100, "bar"));
    expect(plannerKeys.list(100, "foo").slice(0, 2)).toEqual(plannerKeys.listPrefix());
    expect(plannerKeys.list(100, " ")).toEqual(plannerKeys.list(100));
  });
  it("returns pairwise-distinct keys per resource so disabled queries cannot alias", () => {
    // The keys are tuples; flattening to JSON gives a sound equality
    // check that mirrors how TanStack Query hashes them in practice.
    const sentinels = [
      JSON.stringify(plannerKeys.pendingDetail()),
      JSON.stringify(plannerKeys.pendingMessages()),
      JSON.stringify(plannerKeys.pendingPlan()),
      JSON.stringify(plannerKeys.pendingVariants()),
      JSON.stringify(plannerKeys.pendingOperation()),
    ];
    expect(new Set(sentinels).size).toBe(sentinels.length);
  });
});
