import { describe, expect, it } from "vitest";
import type { RunNodeExecution } from "@/api/hooks";
import { presentRunNodeExecution, presentRecoveryTimeline } from "./run-execution-presentation";

const node = (overrides: Partial<RunNodeExecution>): Pick<RunNodeExecution, "status" | "failure" | "recovery"> => ({ status: "failed", ...overrides });

describe("run execution presentation", () => {
  it("keeps canonical cancellation above retained failure and recovery diagnostics", () => {
    const presentation = presentRunNodeExecution(node({ status: "cancelled", failure: { code: "provider_cancelled", classification: "permanent", category: "provider", retryable: false, operator_text: "Guardian recorded provider cancellation." }, recovery: { phase: "exhausted", trigger: "automatic_retry", reason_code: "provider_cancelled", source_attempt_id: "a", source_node_run_id: "n", retry_index: 1, retry_max: 3, scheduled_at: "2026-03-01T10:00:00Z", last_updated_at: "2026-03-01T10:00:00Z" } }), { status: "cancelled", failure_code: "provider_cancelled", failure_text: "Guardian recorded provider cancellation." });
    expect(presentation).toMatchObject({ status: "cancelled", tone: "cancelled", heading: "Cancelled", description: "This step stopped before completion." });
    expect(presentation.requiredAction).toBeUndefined();
    expect(presentation.failureDetail).toBeUndefined();
  });
	it("keeps rate-limit and usage-limit recovery copy and actions distinct", () => {
		const rateLimit = presentRunNodeExecution(node({ recovery: { phase: "scheduled", trigger: "automatic_retry", reason_code: "llm_rate_limited", source_attempt_id: "a", source_node_run_id: "n", retry_index: 1, retry_max: 3, scheduled_at: "2026-03-01T10:00:00Z", last_updated_at: "2026-03-01T10:00:00Z" } }));
		const usageLimit = presentRunNodeExecution(node({ recovery: { phase: "exhausted", trigger: "automatic_retry", reason_code: "llm_usage_limit_reached", source_attempt_id: "a", source_node_run_id: "n", retry_index: 3, retry_max: 3, scheduled_at: "2026-03-01T10:00:00Z", last_updated_at: "2026-03-01T10:00:00Z" } }));
		expect(rateLimit).toMatchObject({ heading: "Retry Scheduled", timeline: { label: "Retry Scheduled - Rate Limit" } });
		expect(usageLimit).toMatchObject({ heading: "Usage Limit Reached", requiredAction: "Required Action: Restore provider capacity or select another model, then retry this step." });
	});

	it("maps every durable automatic recovery phase to its Phase 9 milestone", () => {
		const base = { trigger: "automatic_retry" as const, reason_code: "timeout", retry_index: 1, retry_max: 3, scheduled_at: "2026-03-01T10:00:00Z" };
		expect(["scheduled", "paused", "running", "recovered", "exhausted"].map((phase) => presentRecoveryTimeline({ ...base, phase: phase as "scheduled" | "paused" | "running" | "recovered" | "exhausted" }).label)).toEqual(["Retry Scheduled - Timeout", "Retry Paused - Previous Attempt Still Running", "Retry Started - Attempt 2", "Retry Recovered - Attempt 2", "Retries Exhausted - Timeout"]);
	});
});
