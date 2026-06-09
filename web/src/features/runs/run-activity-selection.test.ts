import { describe, expect, it } from "vitest";
import type { RunExecution } from "@/api/hooks";
import { buildActivityStepGroups, preferredActivityAttempt } from "./run-activity-selection";

function makeExecution(overrides: Partial<RunExecution> = {}): RunExecution {
	return {
		attempt_id: "attempt-1",
		node_run_id: "nr-1",
		node_id: "advisor_consultation_peasant",
		attempt: 1,
		agent_id: "agent-1",
		required_role: "peasant-delegate",
		execution_mode: "provider_backed",
		status: "succeeded",
		queued_at: "2026-03-01T10:08:00Z",
		activity_at: "2026-03-01T10:09:00Z",
		updated_at: "2026-03-01T10:09:00Z",
		committed_at: "2026-03-01T10:09:00Z",
		...overrides,
	};
}

describe("run activity selection", () => {
	it("prefers a blocked lineage over a newer queued retry", () => {
		const preferred = preferredActivityAttempt([
			makeExecution({ attempt_id: "attempt-blocked", node_run_id: "nr-blocked", status: "blocked", activity_at: "2026-03-01T10:08:00Z" }),
			makeExecution({ attempt_id: "attempt-queued", node_run_id: "nr-queued", attempt: 2, status: "queued", activity_at: "2026-03-01T10:09:00Z", committed_at: undefined }),
		]);

		expect(preferred?.attempt_id).toBe("attempt-blocked");
	});

	it("prefers a launched replacement over a blocked lineage", () => {
		const preferred = preferredActivityAttempt([
			makeExecution({ attempt_id: "attempt-blocked", node_run_id: "nr-blocked", status: "blocked", activity_at: "2026-03-01T10:08:00Z" }),
			makeExecution({ attempt_id: "attempt-starting", node_run_id: "nr-starting", attempt: 2, status: "starting", activity_at: "2026-03-01T10:09:00Z", committed_at: undefined }),
		]);

		expect(preferred?.attempt_id).toBe("attempt-starting");
	});

	it("builds step groups from the preferred lineage instead of raw recency", () => {
		const [group] = buildActivityStepGroups([
			makeExecution({
				attempt_id: "attempt-blocked",
				node_run_id: "nr-blocked",
				node_id: "delegated_execution_berserker",
				required_role: "builder-berserker",
				status: "blocked",
				activity_at: "2026-03-01T10:08:00Z",
				updated_at: "2026-03-01T10:08:00Z",
				committed_at: undefined,
			}),
			makeExecution({
				attempt_id: "attempt-queued",
				node_run_id: "nr-queued",
				node_id: "delegated_execution_berserker",
				attempt: 2,
				required_role: "builder-berserker",
				status: "queued",
				activity_at: "2026-03-01T10:09:00Z",
				updated_at: "2026-03-01T10:09:00Z",
				committed_at: undefined,
			}),
		]);

		expect(group?.preferredAttemptId).toBe("attempt-blocked");
		expect(group?.activityAt).toBe("2026-03-01T10:08:00Z");
	});
});
