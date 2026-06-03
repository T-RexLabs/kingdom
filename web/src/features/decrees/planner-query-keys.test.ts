import { describe, expect, it } from "vitest";
import { plannerKeys } from "./planner-query-keys";

describe("plannerKeys pending sentinels", () => {
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
