import { describe, expect, it } from "vitest";
import { getStatusBg, nodeExecutionSemantics } from "./utils";

describe("agent status color mappings", () => {
	it("maps idle and busy badge styles to the unified palette", () => {
		expect(getStatusBg("idle")).toBe("bg-status-blocked/15 text-status-blocked");
		expect(getStatusBg("busy")).toBe("bg-status-running/15 text-status-running");
	});

	it("maps committed execution states to terminal badge styles", () => {
		expect(getStatusBg("committed_success")).toBe("bg-status-succeeded/15 text-status-succeeded");
		expect(getStatusBg("committed_failure")).toBe("bg-status-failed/15 text-status-failed");
		expect(getStatusBg("committed_cancelled")).toBe("bg-status-cancelled/15 text-status-cancelled");
	});
});

describe("node execution semantics", () => {
  it("maps runtime-owned nodes to crown-handled mode", () => {
    expect(nodeExecutionSemantics("runtime_owned", "king")).toEqual({
      executionModeLabel: "Crown-handled",
      roleLabel: "King",
    });
  });

  it("maps provider-backed nodes to agent-executed mode", () => {
    expect(nodeExecutionSemantics("provider_backed", "builder-berserker")).toEqual({
      executionModeLabel: "Agent-executed",
      roleLabel: "Builder Berserker",
    });
  });

  it("falls back cleanly for unknown ownership and empty roles", () => {
    expect(nodeExecutionSemantics("mystery_mode", "")).toEqual({
      executionModeLabel: "Execution mode unknown",
      roleLabel: "Unknown Role",
    });
  });
});
