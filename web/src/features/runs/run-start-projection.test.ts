import { describe, expect, it } from "vitest";
import { buildRunSummaryFromCollectionEvent, readRunStartProjection } from "./run-cache-updates";

describe("run start projection", () => {
	it("accepts only an observed server timestamp and never substitutes creation or launch", () => {
		expect(readRunStartProjection({ start_state: "observed", started_at: "2026-04-30T12:00:01Z" })).toEqual({ start_state: "observed", started_at: "2026-04-30T12:00:01Z" });
		for (const started_at of [undefined, "invalid"]) {
			expect(readRunStartProjection({ start_state: "observed", started_at, created_at: "2026-04-30T12:00:00Z", launch_started_at: "2026-04-30T12:00:00Z" })).toBeNull();
		}
	});

	it("keeps missing observation distinct from never-started execution", () => {
		expect(readRunStartProjection({ start_state: "unrecorded" })).toEqual({ start_state: "unrecorded", started_at: undefined });
		expect(readRunStartProjection({ start_state: "not_started" })).toEqual({ start_state: "not_started", started_at: undefined });
		expect(readRunStartProjection({ start_state: "not_started", started_at: "2026-04-30T12:00:01Z" })).toBeNull();
		expect(readRunStartProjection({})).toBeNull();
	});

	it("carries the authoritative start evidence through collection events", () => {
		const summary = buildRunSummaryFromCollectionEvent({
			run_id: "run", workflow_id: "workflow", decree_id: "decree", decree_title: "Work",
			status: "running", plan_state: "approved", display_state: "running",
			created_at: "2026-04-30T12:00:00Z", updated_at: "2026-04-30T12:01:00Z", last_activity_at: "2026-04-30T12:01:00Z",
			start_state: "observed", started_at: "2026-04-30T12:00:01Z",
		});
		expect(summary?.started_at).toBe("2026-04-30T12:00:01Z");
		expect(summary?.start_state).toBe("observed");
	});
});
