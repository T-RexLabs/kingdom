import { describe, expect, it } from "vitest";
import { attemptEventIconKey, liveExecutionLabel, timelineEventTone } from "./run-status-semantics";

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
			event_type: "attempt.retry_exhausted",
			reason_code: "provider_empty_output_exhausted",
			payload_json: {},
		})).toBe("alert");
		expect(liveExecutionLabel("provider_invoked", "attempt.progress")).toBe("Provider running");
	});
});
