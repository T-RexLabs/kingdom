import { describe, expect, it } from "vitest";
import { statusBadgeClass, statusLabel, statusTextClass, statusToneForKey } from "./status-system";

describe("statusLabel", () => {
	it("labels the working state 'Active' for both agents and nodes/runs", () => {
		expect(statusLabel("busy")).toBe("Active");
		expect(statusLabel("running")).toBe("Active");
	});

	it("keeps distinct sub-states and falls back to title case", () => {
		expect(statusLabel("starting")).toBe("Launching");
		expect(statusLabel("verifying")).toBe("Verifying");
		expect(statusLabel("idle")).toBe("Idle");
		expect(statusLabel("awaiting_plan_decision")).toBe("Awaiting Plan Decision");
		expect(statusLabel("paused")).toBe("Retry Paused");
		expect(statusLabel("some_new_state")).toBe("Some New State");
	});
});

describe("status system", () => {
	it("keeps cancelled and rework on distinct tones", () => {
		expect(statusToneForKey("cancelled")).toBe("cancelled");
		expect(statusToneForKey("rework")).toBe("rework");
	});

	it("uses distinct semantic badges for cancelled and rework", () => {
		expect(statusBadgeClass("cancelled", "strong")).toContain("bg-status-cancelled/15");
		expect(statusBadgeClass("cancelled", "strong")).toContain("text-status-cancelled");
		expect(statusBadgeClass("rework", "strong")).toContain("bg-status-rework/15");
		expect(statusBadgeClass("rework", "strong")).toContain("text-status-rework");
		expect(statusTextClass("cancelled")).toBe("text-status-cancelled");
		expect(statusTextClass("rework")).toBe("text-status-rework");
	});

	it("treats ready as attention rather than running", () => {
		expect(statusToneForKey("ready")).toBe("attention");
		expect(statusBadgeClass("ready")).toContain("bg-status-blocked/10");
		expect(statusTextClass("ready")).toBe("text-status-blocked");
	});

	it("treats paused retries as attention", () => {
		expect(statusToneForKey("paused")).toBe("attention");
		expect(statusBadgeClass("paused")).toContain("bg-status-blocked/10");
	});

	it("treats queued as attention so pending work stays visible", () => {
		expect(statusToneForKey("queued")).toBe("attention");
		expect(statusBadgeClass("queued")).toContain("bg-status-blocked/10");
		expect(statusTextClass("queued")).toBe("text-status-blocked");
	});
});
