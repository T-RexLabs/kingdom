import { QueryClient } from "@tanstack/react-query";
import { afterEach, describe, expect, it } from "vitest";
	import {
	mergeRunNodeExecutionItems,
	overlayExecutionDetail,
	overlayExecutionItem,
	patchLiveExecutionProjection,
	readLiveExecutionProjection,
} from "./run-live-execution-projection";

describe("run-live-execution-projection", () => {
	const clients: QueryClient[] = [];

	afterEach(() => {
		for (const client of clients) {
			client.clear();
		}
		clients.length = 0;
	});

	function makeClient(): QueryClient {
		const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
		clients.push(client);
		return client;
	}

	it("keeps a terminal live status from regressing to running on later telemetry", () => {
		const queryClient = makeClient();

		patchLiveExecutionProjection(queryClient, "run-1", {
			node_run_id: "nr-1",
			activity_at: "2026-03-01T10:08:00Z",
			status: "succeeded",
			terminal_observed_at: "2026-03-01T10:08:00Z",
			live_label: "Execution succeeded",
		});
		patchLiveExecutionProjection(queryClient, "run-1", {
			node_run_id: "nr-1",
			activity_at: "2026-03-01T10:09:00Z",
			status: "running",
			live_label: "Completion observed",
			live_reason_code: "runtime_completion_observation",
		});

		expect(readLiveExecutionProjection(queryClient, "run-1")).toEqual({
			"nr-1": {
				node_run_id: "nr-1",
				activity_at: "2026-03-01T10:08:00Z",
				status: "succeeded",
				terminal_observed_at: "2026-03-01T10:08:00Z",
				live_label: "Execution succeeded",
			},
		});
	});

	it("accepts later terminal telemetry for an in-flight live status", () => {
		const queryClient = makeClient();

		patchLiveExecutionProjection(queryClient, "run-1", {
			node_run_id: "nr-1",
			activity_at: "2026-03-01T10:07:00Z",
			status: "running",
			started_at: "2026-03-01T10:07:00Z",
			live_label: "Provider running",
		});
		patchLiveExecutionProjection(queryClient, "run-1", {
			node_run_id: "nr-1",
			activity_at: "2026-03-01T10:08:00Z",
			status: "succeeded",
			terminal_observed_at: "2026-03-01T10:08:00Z",
			live_label: "Execution succeeded",
		});

		expect(readLiveExecutionProjection(queryClient, "run-1")).toEqual({
			"nr-1": {
				node_run_id: "nr-1",
				activity_at: "2026-03-01T10:08:00Z",
				status: "succeeded",
				started_at: "2026-03-01T10:07:00Z",
				terminal_observed_at: "2026-03-01T10:08:00Z",
				live_label: "Execution succeeded",
			},
		});
	});

	it("prefers a later same-timestamp terminal event when run event ordering advances", () => {
		const queryClient = makeClient();

		patchLiveExecutionProjection(queryClient, "run-1", {
			node_run_id: "nr-1",
			activity_at: "2026-03-01T10:08:00Z",
			run_event_seq: 140,
			attempt_seq: 1,
			status: "running",
			started_at: "2026-03-01T10:08:00Z",
			live_label: "Execution started",
		});
		patchLiveExecutionProjection(queryClient, "run-1", {
			node_run_id: "nr-1",
			activity_at: "2026-03-01T10:08:00Z",
			run_event_seq: 143,
			attempt_seq: 2,
			status: "succeeded",
			terminal_observed_at: "2026-03-01T10:08:00Z",
			live_label: "Execution succeeded",
		});

		expect(readLiveExecutionProjection(queryClient, "run-1")).toEqual({
			"nr-1": {
				node_run_id: "nr-1",
				activity_at: "2026-03-01T10:08:00Z",
				run_event_seq: 143,
				attempt_seq: 2,
				status: "succeeded",
				started_at: "2026-03-01T10:08:00Z",
				terminal_observed_at: "2026-03-01T10:08:00Z",
				live_label: "Execution succeeded",
			},
		});
	});

	it("does not let stale live running status override canonical terminal execution state", () => {
		const execution = {
			attempt_id: "attempt-1",
			node_run_id: "nr-1",
			node_id: "execution_kickoff",
			attempt: 1,
			agent_id: "agent-council",
			required_role: "plan-author",
			execution_mode: "runtime_owned",
			status: "succeeded",
			queued_at: "2026-03-01T10:08:00Z",
			activity_at: "2026-03-01T10:08:00Z",
			updated_at: "2026-03-01T10:08:00Z",
			terminal_observed_at: "2026-03-01T10:08:00Z",
			committed_at: "2026-03-01T10:08:00Z",
			stop_retry_available: false,
		} as const;

		expect(overlayExecutionItem(execution, {
			node_run_id: "nr-1",
			activity_at: "2026-03-01T10:09:00Z",
			status: "running",
			live_label: "Execution started",
		})).toEqual(execution);

		expect(overlayExecutionDetail({
			...execution,
			settlement_status: "not_applicable",
			output_status: "none",
		}, {
			node_run_id: "nr-1",
			activity_at: "2026-03-01T10:09:00Z",
			status: "running",
			live_label: "Execution started",
		})).toEqual({
			...execution,
			settlement_status: "not_applicable",
			output_status: "none",
		});
	});

	it("does not let stale live launch status override fresher api execution state", () => {
		const execution = {
			attempt_id: "attempt-2",
			node_run_id: "nr-2",
			node_id: "implementation",
			attempt: 2,
			agent_id: "agent-builder",
			required_role: "builder",
			execution_mode: "provider_backed",
			status: "verifying",
			queued_at: "2026-03-01T10:08:00Z",
			activity_at: "2026-03-01T10:08:10Z",
			updated_at: "2026-03-01T10:08:10Z",
			launch_started_at: "2026-03-01T10:08:00Z",
			stop_retry_available: false,
			started_at: "2026-03-01T10:08:03Z",
			provider_session_id: "ses-2",
			provider_invocation_id: "inv-2",
			settlement_status: "pending",
			output_status: "none",
		} as const;

		expect(overlayExecutionItem({
			attempt_id: execution.attempt_id,
			node_run_id: execution.node_run_id,
			node_id: execution.node_id,
			attempt: execution.attempt,
			agent_id: execution.agent_id,
			required_role: execution.required_role,
			execution_mode: execution.execution_mode,
			status: execution.status,
			queued_at: execution.queued_at,
			activity_at: execution.activity_at,
			updated_at: execution.updated_at,
			launch_started_at: execution.launch_started_at,
			stop_retry_available: execution.stop_retry_available,
			started_at: execution.started_at,
			provider_session_id: execution.provider_session_id,
		}, {
			node_run_id: "nr-2",
			activity_at: "2026-03-01T10:08:05Z",
			status: "starting",
			live_label: "Launching provider",
		})).toMatchObject({
			status: "verifying",
			activity_at: "2026-03-01T10:08:10Z",
		});

		expect(overlayExecutionDetail(execution, {
			node_run_id: "nr-2",
			activity_at: "2026-03-01T10:08:05Z",
			status: "starting",
			live_label: "Launching provider",
		})).toMatchObject({
			status: "verifying",
			activity_at: "2026-03-01T10:08:10Z",
		});
	});

	it("overlays completion observation as verifying detail while settlement is still pending", () => {
		expect(overlayExecutionDetail({
			attempt_id: "attempt-1",
			node_run_id: "nr-1",
			node_id: "implementation",
			attempt: 1,
			agent_id: "agent-builder",
			required_role: "builder",
			execution_mode: "provider_backed",
			status: "running",
			queued_at: "2026-03-01T10:08:00Z",
			activity_at: "2026-03-01T10:08:00Z",
			updated_at: "2026-03-01T10:08:00Z",
			provider_session_id: "ses-1",
			stop_retry_available: false,
			provider_invocation_id: "inv-1",
			settlement_status: "pending",
			output_status: "none",
		}, {
			node_run_id: "nr-1",
			activity_at: "2026-03-01T10:08:05Z",
			status: "verifying",
			live_label: "Completion observed",
			live_reason_code: "runtime_completion_observation",
		})).toMatchObject({
			status: "verifying",
			activity_at: "2026-03-01T10:08:05Z",
			live_signal: {
				live_label: "Completion observed",
				live_reason_code: "runtime_completion_observation",
			},
		});
	});

	it("does not synthesize logical nodes from SSE before the executions refetch", () => {
		const merged = mergeRunNodeExecutionItems([], {
			"nr-live-only": {
				node_run_id: "nr-live-only",
				attempt_id: "attempt-live-only",
				node_id: "implementation",
				attempt: 2,
				required_role: "builder",
				execution_mode: "provider_backed",
				activity_at: "2026-03-01T10:09:00Z",
				status: "running",
				started_at: "2026-03-01T10:09:00Z",
				live_label: "Execution started",
			},
		});

		expect(merged).toEqual([]);
	});

	it("overlays only a matching canonical current execution", () => {
		const [merged] = mergeRunNodeExecutionItems([{
			node_id: "implementation",
			required_role: "builder",
			execution_ownership: "provider_backed",
			status: "running",
			attempt_count: 1,
			current_execution: {
				attempt_id: "attempt-1", node_run_id: "nr-1", node_id: "implementation", attempt: 1,
				agent_id: "agent-1", required_role: "builder", execution_mode: "provider_backed", status: "running",
				queued_at: "2026-03-01T10:08:00Z", activity_at: "2026-03-01T10:08:00Z", updated_at: "2026-03-01T10:08:00Z", stop_retry_available: false,
			},
		}], {
			"nr-other": { node_run_id: "nr-other", attempt_id: "attempt-other", activity_at: "2026-03-01T10:09:00Z", status: "succeeded" },
		});

		expect(merged?.current_execution?.status).toBe("running");
	});

	it("preserves earliest observed timestamps across newer live updates", () => {
		const queryClient = makeClient();

		patchLiveExecutionProjection(queryClient, "run-1", {
			node_run_id: "nr-1",
			activity_at: "2026-03-01T10:09:00Z",
			status: "starting",
			launch_started_at: "2026-03-01T10:09:00Z",
			started_at: "2026-03-01T10:10:00Z",
		});
		patchLiveExecutionProjection(queryClient, "run-1", {
			node_run_id: "nr-1",
			activity_at: "2026-03-01T10:10:02Z",
			status: "running",
			launch_started_at: "2026-03-01T10:09:02Z",
			started_at: "2026-03-01T10:10:02Z",
			terminal_observed_at: "2026-03-01T10:12:00Z",
		});

		expect(readLiveExecutionProjection(queryClient, "run-1")).toEqual({
			"nr-1": {
				node_run_id: "nr-1",
				activity_at: "2026-03-01T10:10:02Z",
				status: "running",
				launch_started_at: "2026-03-01T10:09:00Z",
				started_at: "2026-03-01T10:10:00Z",
				terminal_observed_at: "2026-03-01T10:12:00Z",
			},
		});
	});
});
