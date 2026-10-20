import { describe, expect, it } from "vitest";
import type { RunGraphNode } from "@/api/hooks";
import { selectRunRecovery } from "./run-recovery-selection";

function node(node_id: string, state: RunGraphNode["state"]): RunGraphNode {
  return { node_id, state, required_role: "coordinator", execution_ownership: "provider_backed" };
}
const edges = [{ from: "intake", to: "synthesis" }, { from: "synthesis", to: "draft" }, { from: "draft", to: "review" }];

describe("run recovery selection", () => {
  it.each(["failed", "cancelled"])("recovers an interrupted %s run from its earliest cancelled step", (status) => {
    const nodes = [node("intake", "succeeded"), node("synthesis", "cancelled"), node("draft", "cancelled"), node("review", "cancelled")];
    expect(selectRunRecovery(status, nodes, edges)).toEqual({ nodes: nodes.slice(1), defaultNodeId: "synthesis" });
  });

  it("keeps failed and cancelled targets together and defaults before downstream cancellation", () => {
    const nodes = [node("intake", "succeeded"), node("synthesis", "failed"), node("draft", "cancelled"), node("review", "cancelled")];
    expect(selectRunRecovery("failed", nodes, edges)).toEqual({ nodes: nodes.slice(1), defaultNodeId: "synthesis" });
  });

  it("follows dependencies through skipped nodes and ignores rework feedback", () => {
    const nodes = [node("intake", "cancelled"), node("synthesis", "skipped"), node("draft", "cancelled")];
    expect(selectRunRecovery("failed", nodes, [...edges, { from: "draft", to: "intake", on: "rework" }]).defaultNodeId).toBe("intake");
  });

  it("requires a choice between independent interrupted branches", () => {
    const nodes = [node("left", "failed"), node("right", "cancelled"), node("join", "cancelled")];
    expect(selectRunRecovery("failed", nodes, [{ from: "left", to: "join" }, { from: "right", to: "join" }]).defaultNodeId).toBeUndefined();
  });

  it.each(["running", "queued", "succeeded", "awaiting_human_confirmation"])("does not recover a %s run", (status) => {
    expect(selectRunRecovery(status, [node("draft", "cancelled")], [])).toEqual({ nodes: [] });
  });
});
