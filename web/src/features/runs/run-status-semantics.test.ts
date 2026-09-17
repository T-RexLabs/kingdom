import { describe, expect, it } from "vitest";
import { attemptEventIconKey, liveExecutionLabel, operatorExecutionStatusText, timelineEventTone } from "./run-status-semantics";

describe("run status semantics", () => {
	it("treats run cancellations as cancelled instead of failed", () => {
		expect(timelineEventTone({
			event_type: "run.state_changed",
			reason_code: "operator_cancelled",
			payload_json: { action: "cancel", status: "cancelled" },
		})).toBe("cancelled");
	});

	it("keeps retry queue and progress events on deliberate tones", () => {
		expect(timelineEventTone({
			event_type: "attempt.retry_queued",
			reason_code: "agent_unresponsive",
			payload_json: {},
		})).toBe("ready");
		expect(timelineEventTone({
			event_type: "attempt.progress",
			reason_code: "provider_invoked",
			payload_json: {},
		})).toBe("running");
		expect(attemptEventIconKey({
			event_type: "attempt.retry_scheduled",
			reason_code: "provider_empty_output",
			payload_json: {},
		})).toBe("rotate");
		expect(liveExecutionLabel("provider_invoked", "attempt.progress")).toBe("Provider running");
		expect(liveExecutionLabel(undefined, "node.state_changed", "blocked")).toBe("Blocked");
	});

	it("distinguishes launching, running, and verifying operator copy", () => {
		expect(operatorExecutionStatusText("starting")).toBe("This step is launching now.");
		expect(operatorExecutionStatusText("running")).toBe("This step is still running.");
		expect(operatorExecutionStatusText("blocked")).toBe("This step is blocked while Kingdom decides or performs recovery.");
		expect(operatorExecutionStatusText("blocked", "Blocked")).toBe("This step is blocked while Kingdom decides or performs recovery.");
		expect(operatorExecutionStatusText("paused")).toBe("Kingdom is waiting for the previous attempt to stop before starting the retry.");
		expect(operatorExecutionStatusText("paused", undefined, "stop_retry_requested")).toBe("Kingdom is sending another stop request before retrying this step.");
		expect(operatorExecutionStatusText("paused", undefined, "stop_retry_dispatched")).toBe("Kingdom is confirming that the previous stop request completed.");
		expect(operatorExecutionStatusText("paused", "Provider running", "workspace_restore_failed")).toBe("Kingdom stopped the previous attempt but could not restore the trusted workspace.");
		expect(operatorExecutionStatusText("paused", "Provider running", "awaiting_workspace_quiescence")).toBe("Kingdom is waiting for another active step to release the shared workspace.");
		expect(operatorExecutionStatusText("verifying")).toBe("Kingdom is still confirming the final result for this step.");
	});
});
