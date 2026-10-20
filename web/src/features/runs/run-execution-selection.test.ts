import { describe, expect, it } from "vitest";
import type { RunExecution, RunNodeExecution } from "@/api/hooks";
import { followedAttemptId, selectRunExecutionNode } from "./run-execution-selection";

function node(
  id: string,
  status?: RunExecution["status"],
  at = "2026-09-30T10:00:00Z",
): RunNodeExecution {
  return {
    node_id: id,
    required_role: "builder",
    execution_ownership: "provider_backed",
    status:
      status === "queued"
        ? "pending"
        : status === "verifying"
          ? "running"
          : status === "paused"
            ? "blocked"
            : (status ?? "pending"),
    attempt_count: status ? 1 : 0,
    ...(status
      ? {
          current_execution: {
            node_id: id,
            attempt_id: `${id}-attempt`,
            execution_mode: "provider_backed",
            status,
            activity_at: at,
          } as RunExecution,
        }
      : {}),
  };
}
describe("run conversation selection", () => {
  it("preserves an explicit selection even when it has not started", () => {
    const nodes = [node("future"), node("working", "running")];
    expect(selectRunExecutionNode(nodes, "future")?.node_id).toBe("future");
  });
  it.each(["starting", "running", "verifying"] as const)(
    "prefers an active %s execution",
    (status) => {
      expect(
        selectRunExecutionNode([
          node("future"),
          node("finished", "succeeded", "2026-09-30T11:00:00Z"),
          node("working", status),
        ])?.node_id,
      ).toBe("working");
    },
  );
  it("chooses the latest recorded provider work ahead of a newer queued attempt or runtime step", () => {
    const nodes = [
      node("future", "queued", "2026-09-30T12:00:00Z"),
      node("older", "succeeded"),
      node("latest", "failed", "2026-09-30T11:00:00Z"),
      {
        ...node("runtime", "succeeded", "2026-09-30T13:00:00Z"),
        execution_ownership: "runtime_owned" as const,
      },
    ];
    expect(selectRunExecutionNode(nodes)?.node_id).toBe("latest");
  });
  it("uses a stable node identity tie-break when timestamps are equal or missing", () => {
    const nodes = [node("b", "succeeded"), node("a", "succeeded")];
    expect(selectRunExecutionNode(nodes)?.node_id).toBe("a");
    expect(selectRunExecutionNode([...nodes].reverse())?.node_id).toBe("a");
    expect(
      selectRunExecutionNode([
        node("b", "failed", "invalid"),
        node("a", "failed", "invalid"),
      ])?.node_id,
    ).toBe("a");
  });
  it("falls back honestly when no provider attempt exists", () => {
    expect(selectRunExecutionNode([node("future")])?.node_id).toBe("future");
    expect(selectRunExecutionNode([])).toBeNull();
  });
});

describe("followedAttemptId", () => {
  const step = (overrides: Partial<RunNodeExecution>): RunNodeExecution => ({
    node_id: "implementation",
    required_role: "builder",
    execution_ownership: "provider_backed",
    status: "pending",
    attempt_count: 0,
    ...overrides,
  });
  const attempts = [
    { attempt_id: "second", attempt: 2 },
    { attempt_id: "third", attempt: 3 },
    { attempt_id: "first", attempt: 1 },
  ];

  it("follows the current attempt, else the latest recorded one, else none", () => {
    expect(followedAttemptId(step({ attempt_count: 4, current_execution: { attempt_id: "fourth" } as RunNodeExecution["current_execution"] }), attempts)).toBe("fourth");
    expect(followedAttemptId(step({ attempt_count: 3 }), attempts)).toBe("third");
    expect(followedAttemptId(step({ attempt_count: 3 }), [])).toBeNull();
    expect(followedAttemptId(step({}), attempts)).toBeNull();
    expect(followedAttemptId(null, attempts)).toBeNull();
  });
});
