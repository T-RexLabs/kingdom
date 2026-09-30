import { describe, expect, it } from "vitest";
import { layoutRunGraph, observedProviderRunning } from "./run-graph-model";

describe("run graph evidence", () => {
  it("animates only observed provider execution with a current live connection", () => {
    const running = {
      execution_mode: "provider_backed",
      status: "running",
      started_at: "2026-09-29T10:00:00Z",
    };
    expect(observedProviderRunning(running, true)).toBe(true);
    for (const status of [
      "queued",
      "starting",
      "verifying",
      "paused",
      "succeeded",
    ]) {
      expect(observedProviderRunning({ ...running, status }, true)).toBe(false);
    }
    expect(
      observedProviderRunning({ ...running, started_at: undefined }, true),
    ).toBe(false);
    expect(
      observedProviderRunning(
        { ...running, execution_mode: "runtime_owned" },
        true,
      ),
    ).toBe(false);
    expect(
      observedProviderRunning(
        { ...running, handoff_state: "cancellation_requested" },
        true,
      ),
    ).toBe(false);
    expect(observedProviderRunning(running, false)).toBe(false);
  });

  it("keeps parallel branches and joins aligned while preserving a rework edge", () => {
    const nodes = ["start", "left", "right", "join"].map((node_id) => ({
      node_id,
      state: "pending" as const,
      required_role: "builder",
      execution_ownership: "provider_backed" as const,
    }));
    const edges = [
      { from: "start", to: "left" },
      { from: "start", to: "right" },
      { from: "left", to: "join" },
      { from: "right", to: "join" },
      { from: "join", to: "start", on: "rework" },
    ];
    const layout = layoutRunGraph({ run_id: "run", nodes, edges });
    expect(layout.nodes[1].y).toBe(layout.nodes[2].y);
    expect(layout.nodes[3].y).toBeGreaterThan(layout.nodes[1].y);
    expect(layout.edges).toHaveLength(edges.length);
    expect(layout.edges.find((edge) => edge.on === "rework")?.backward).toBe(
      true,
    );
    expect(layout.width).toBeLessThanOrEqual(800);
  });
});
