import { describe, expect, it } from "vitest";
import { agentAvailability } from "./agent-availability";

describe("global agent availability", () => {
  it("aggregates simultaneous instances without selecting a run", () => {
    expect(
      agentAvailability([
        { status: "busy" },
        { status: "busy" },
        { status: "idle" },
      ]),
    ).toEqual({
      label: "Running",
      tone: "running",
      details: "2 Running · 1 Available",
    });
  });
  it("does not hide failures behind available or running instances", () => {
    expect(
      agentAvailability([{ status: "idle" }, { status: "failed" }]),
    ).toMatchObject({ label: "Mixed", tone: "attention" });
    expect(
      agentAvailability([{ status: "busy" }, { status: "blocked" }]),
    ).toMatchObject({ label: "Mixed", tone: "attention" });
  });
  it("keeps unassigned, offline, blocked and unknown distinct", () => {
    expect(agentAvailability([]).label).toBe("Unassigned");
    expect(agentAvailability([{ status: "offline" }]).label).toBe("Offline");
    expect(agentAvailability([{ status: "blocked" }]).label).toBe("Blocked");
    expect(agentAvailability([{ status: "new-provider-state" }]).label).toBe(
      "Unknown",
    );
  });
});
