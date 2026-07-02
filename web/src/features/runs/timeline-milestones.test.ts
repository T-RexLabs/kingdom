import { describe, expect, it } from "vitest";
import type { RunExecution, RunGraphNode, TimelineEvent } from "@/api/hooks-types";
import { buildRawEventLabel, buildTimelineLookups, synthesizeMilestones } from "./timeline-milestones";

describe("timeline milestones", () => {
	it("describes runtime-owned raw events without default execution metadata", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "king_plan_decision",
      required_role: "king",
      execution_ownership: "runtime_owned",
      state: "pending",
    }];
    const events: TimelineEvent[] = [{
      event_id: "evt-approval-request",
      run_event_seq: 1,
      attempt_seq: 1,
      event_type: "mailbox.message_recorded",
      emitted_at: "2026-03-18T11:40:00Z",
      payload_json: {
        node_id: "king_plan_decision",
        communication_type: "approval_request",
        visible_to_operator: true,
      },
    }];

    const lookups = buildTimelineLookups(events, nodes);

		expect(buildRawEventLabel(events[0], lookups)).toEqual({
			label: "King Plan Decision awaiting approval",
			detail: undefined,
		});
	});

	it("keeps provider-backed node milestones focused on outcome", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "delegated_execution_berserker",
      required_role: "builder-berserker",
      execution_ownership: "provider_backed",
      state: "succeeded",
    }];
    const events: TimelineEvent[] = [
      {
        event_id: "evt-start",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "attempt.started",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "delegated_execution_berserker" },
      },
      {
        event_id: "evt-success",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.succeeded",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "delegated_execution_berserker" },
      },
    ];

    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-delegated_execution_berserker",
				label: "Delegated Execution Berserker",
				detail: "Succeeded",
			}),
		]));
	});

	it("labels retry and progress attempt events with runtime semantics instead of generic fallbacks", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "delegated_execution_berserker",
			required_role: "builder-berserker",
			execution_ownership: "provider_backed",
			state: "running",
		}];
		const retryQueued: TimelineEvent = {
			event_id: "evt-retry-queued",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "attempt.retry_queued",
			emitted_at: "2026-03-18T11:40:00Z",
			payload_json: { node_id: "delegated_execution_berserker" },
		};
		const progress: TimelineEvent = {
			event_id: "evt-progress",
			run_event_seq: 2,
			attempt_seq: 2,
			event_type: "attempt.progress",
			reason_code: "provider_invoked",
			emitted_at: "2026-03-18T11:41:00Z",
			payload_json: { node_id: "delegated_execution_berserker" },
		};

		const lookups = buildTimelineLookups([retryQueued, progress], nodes);

		expect(buildRawEventLabel(retryQueued, lookups)).toEqual({
			label: "Delegated Execution Berserker retry queued",
			detail: undefined,
		});
		expect(buildRawEventLabel(progress, lookups)).toEqual({
			label: "Delegated Execution Berserker Provider running",
			detail: undefined,
		});
	});

	it("renders blocked recovery labels and milestones from live execution state", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "delegated_execution_berserker",
			node_run_id: "nr-builder-1",
			required_role: "builder-berserker",
			execution_ownership: "provider_backed",
			state: "running",
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-builder-1",
			node_run_id: "nr-builder-1",
			node_id: "delegated_execution_berserker",
			attempt: 1,
			agent_id: "agent-builder",
			required_role: "builder-berserker",
			execution_mode: "provider_backed",
			status: "blocked",
			queued_at: "2026-03-18T11:39:30Z",
			activity_at: "2026-03-18T11:41:00Z",
			updated_at: "2026-03-18T11:41:00Z",
			launch_started_at: "2026-03-18T11:39:30Z",
		}];
		const recovered: TimelineEvent = {
			event_id: "evt-recovered-blocked",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "attempt.recovered",
			emitted_at: "2026-03-18T11:41:00Z",
			payload_json: { node_id: "delegated_execution_berserker", node_run_id: "nr-builder-1", state: "blocked" },
		};

		const lookups = buildTimelineLookups([recovered], nodes, executions);
		expect(buildRawEventLabel(recovered, lookups)).toEqual({
			label: "Delegated Execution Berserker blocked for retry",
			detail: undefined,
		});

		const milestones = synthesizeMilestones([recovered], nodes, [], lookups);
		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-delegated_execution_berserker",
				status: "blocked",
				kind: "node_blocked",
				detail: "Blocked",
			}),
		]));
	});

	it("keeps blocked recovery visible while still showing the queued retry handoff", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "delegated_execution_berserker",
			node_run_id: "nr-builder-1",
			required_role: "builder-berserker",
			execution_ownership: "provider_backed",
			state: "blocked",
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-builder-1",
			node_run_id: "nr-builder-1",
			node_id: "delegated_execution_berserker",
			attempt: 1,
			agent_id: "agent-builder",
			required_role: "builder-berserker",
			execution_mode: "provider_backed",
			status: "blocked",
			queued_at: "2026-03-18T11:39:30Z",
			activity_at: "2026-03-18T11:42:00Z",
			updated_at: "2026-03-18T11:42:00Z",
			launch_started_at: "2026-03-18T11:39:30Z",
		}];
		const events: TimelineEvent[] = [
			{
				event_id: "evt-recovered-blocked",
				run_event_seq: 5,
				attempt_seq: 3,
				event_type: "attempt.recovered",
				emitted_at: "2026-03-18T11:41:00Z",
				payload_json: { node_id: "delegated_execution_berserker", node_run_id: "nr-builder-1", state: "blocked" },
			},
			{
				event_id: "evt-retry-queued",
				run_event_seq: 6,
				attempt_seq: 1,
				event_type: "attempt.retry_queued",
				emitted_at: "2026-03-18T11:42:00Z",
				payload_json: { node_id: "delegated_execution_berserker", node_run_id: "nr-builder-2", attempt: 2 },
			},
		];

		const lookups = buildTimelineLookups(events, nodes, executions);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-delegated_execution_berserker",
				status: "blocked",
				kind: "node_blocked",
				detail: "Blocked",
				events: expect.arrayContaining([
					expect.objectContaining({ event_id: "evt-recovered-blocked" }),
					expect.objectContaining({ event_id: "evt-retry-queued" }),
				]),
			}),
		]));
	});

	it("keeps blocked lineage state when only the events know about recovery", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "delegated_execution_berserker",
			node_run_id: "nr-builder-1",
			required_role: "builder-berserker",
			execution_ownership: "provider_backed",
			state: "running",
		}];
		const events: TimelineEvent[] = [
			{
				event_id: "evt-recovered-blocked-live-only",
				run_event_seq: 5,
				attempt_seq: 3,
				event_type: "attempt.recovered",
				emitted_at: "2026-03-18T11:41:00Z",
				payload_json: { node_id: "delegated_execution_berserker", node_run_id: "nr-builder-1", state: "blocked" },
			},
			{
				event_id: "evt-retry-queued-live-only",
				run_event_seq: 6,
				attempt_seq: 1,
				event_type: "attempt.retry_queued",
				emitted_at: "2026-03-18T11:42:00Z",
				payload_json: { node_id: "delegated_execution_berserker", node_run_id: "nr-builder-2", attempt: 2 },
			},
		];

		const lookups = buildTimelineLookups(events, nodes, []);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-delegated_execution_berserker",
				status: "blocked",
				kind: "node_blocked",
				detail: "Blocked",
				events: expect.arrayContaining([
					expect.objectContaining({ event_id: "evt-recovered-blocked-live-only" }),
					expect.objectContaining({ event_id: "evt-retry-queued-live-only" }),
				]),
			}),
		]));
	});

	it("anchors launching milestones to launch_started_at before started_at", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "advisor_consultation_burgher",
			node_run_id: "nr-burgher-1",
			required_role: "burgher-delegate",
			execution_ownership: "provider_backed",
			state: "starting",
		}];
		const events: TimelineEvent[] = [{
			event_id: "evt-burgher-old-lineage",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "attempt.failed",
			emitted_at: "2026-03-18T11:38:00Z",
			reason_code: "provider_incomplete_turn",
			payload_json: { node_id: "advisor_consultation_burgher", node_run_id: "nr-burgher-old" },
		}, {
			event_id: "evt-burgher-ready",
			run_event_seq: 2,
			attempt_seq: 1,
			event_type: "node.state_changed",
			emitted_at: "2026-03-18T11:39:00Z",
			payload_json: { node_id: "advisor_consultation_burgher", node_run_id: "nr-burgher-1", state: "ready" },
		}, {
			event_id: "evt-burgher-starting",
			run_event_seq: 3,
			attempt_seq: 2,
			event_type: "node.state_changed",
			emitted_at: "2026-03-18T11:39:30Z",
			payload_json: { node_id: "advisor_consultation_burgher", node_run_id: "nr-burgher-1", state: "starting" },
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-burgher-1",
			node_run_id: "nr-burgher-1",
			node_id: "advisor_consultation_burgher",
			attempt: 1,
			agent_id: "agent-burgher",
			required_role: "burgher-delegate",
			execution_mode: "provider_backed",
			status: "starting",
			queued_at: "2026-03-18T11:39:00Z",
			activity_at: "2026-03-18T11:41:00Z",
			updated_at: "2026-03-18T11:41:00Z",
			launch_started_at: "2026-03-18T11:39:30Z",
			started_at: "2026-03-18T11:40:15Z",
		}];

		const lookups = buildTimelineLookups(events, nodes, executions);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-advisor_consultation_burgher",
				status: "starting",
				detail: "Launching",
				timestamp: "2026-03-18T11:39:30Z",
			}),
		]));
	});

	it("anchors running milestones to provider invocation instead of launch start", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "advisor_consultation_burgher",
			node_run_id: "nr-burgher-running",
			required_role: "burgher-delegate",
			execution_ownership: "provider_backed",
			state: "running",
		}];
		const events: TimelineEvent[] = [{
			event_id: "evt-burgher-starting",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "node.state_changed",
			emitted_at: "2026-03-18T11:39:30Z",
			payload_json: { node_id: "advisor_consultation_burgher", node_run_id: "nr-burgher-running", state: "starting" },
		}, {
			event_id: "evt-burgher-invoked",
			run_event_seq: 2,
			attempt_seq: 2,
			event_type: "attempt.progress",
			reason_code: "provider_invoked",
			emitted_at: "2026-03-18T11:40:15Z",
			payload_json: { node_id: "advisor_consultation_burgher", node_run_id: "nr-burgher-running" },
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-burgher-running",
			node_run_id: "nr-burgher-running",
			node_id: "advisor_consultation_burgher",
			attempt: 1,
			agent_id: "agent-burgher",
			required_role: "burgher-delegate",
			execution_mode: "provider_backed",
			status: "running",
			queued_at: "2026-03-18T11:39:30Z",
			activity_at: "2026-03-18T11:41:00Z",
			updated_at: "2026-03-18T11:41:00Z",
			launch_started_at: "2026-03-18T11:39:30Z",
			progress_at: "2026-03-18T11:40:15Z",
		}];

		const lookups = buildTimelineLookups(events, nodes, executions);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-advisor_consultation_burgher",
				status: "running",
				timestamp: "2026-03-18T11:40:15Z",
			}),
		]));
	});

  it("labels review fan-out cap auto-success with the review badge", () => {
    const nodes: RunGraphNode[] = [
      { node_id: "delegated_execution_berserker", required_role: "builder-berserker", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "execution_review_shieldmaiden_a", required_role: "reviewer-shieldmaiden", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "execution_review_shieldmaiden_b", required_role: "reviewer-shieldmaiden", execution_ownership: "provider_backed", state: "succeeded" },
    ];
    const events: TimelineEvent[] = [
      {
        event_id: "evt-review-a",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "attempt.succeeded",
        reason_code: "execution_review_cap_auto_succeeded",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "execution_review_shieldmaiden_a", node_run_id: "nr-review-a", status: "ok" },
      },
      {
        event_id: "evt-review-b",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.succeeded",
        reason_code: "execution_completed",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "execution_review_shieldmaiden_b", node_run_id: "nr-review-b", status: "ok" },
      },
    ];
    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [
      { from: "delegated_execution_berserker", to: "execution_review_shieldmaiden_a", on: "success" },
      { from: "delegated_execution_berserker", to: "execution_review_shieldmaiden_b", on: "success" },
    ], lookups);

    expect(milestones.find((milestone) => milestone.kind === "fan_out")?.badge).toBe("Review Cap Auto-Succeeded");
  });

  it("uses a generic badge when fan-out mixes review and validation cap auto-success", () => {
    const nodes: RunGraphNode[] = [
      { node_id: "delegated_execution_berserker", required_role: "builder-berserker", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "execution_review_shieldmaiden", required_role: "reviewer-shieldmaiden", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "validation_gates_qa_scout", required_role: "intelligence-qa-scout", execution_ownership: "provider_backed", state: "succeeded" },
    ];
    const events: TimelineEvent[] = [
      {
        event_id: "evt-review",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "attempt.succeeded",
        reason_code: "execution_review_cap_auto_succeeded",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "execution_review_shieldmaiden", node_run_id: "nr-review", status: "ok" },
      },
      {
        event_id: "evt-validation",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.succeeded",
        reason_code: "validation_gate_cap_auto_succeeded",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "validation_gates_qa_scout", node_run_id: "nr-validation", status: "ok" },
      },
    ];
    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [
      { from: "delegated_execution_berserker", to: "execution_review_shieldmaiden", on: "success" },
      { from: "delegated_execution_berserker", to: "validation_gates_qa_scout", on: "success" },
    ], lookups);

    expect(milestones.find((milestone) => milestone.kind === "fan_out")?.badge).toBe("Cap Auto-Succeeded");
  });

  it("shows a node as running when a later attempt starts after an older success", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "validation_gates_nightraven",
      required_role: "intelligence-nightraven",
      execution_ownership: "provider_backed",
      state: "succeeded",
    }];
    const events: TimelineEvent[] = [
      {
        event_id: "evt-success-old",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "attempt.succeeded",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "validation_gates_nightraven" },
      },
      {
        event_id: "evt-start-new",
        run_event_seq: 2,
        attempt_seq: 1,
        event_type: "attempt.started",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "validation_gates_nightraven" },
      },
    ];

    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-validation_gates_nightraven",
				status: "running",
				kind: "node_running",
				detail: "In progress",
			}),
		]));
	});

  it("prefers the latest streamed terminal status over a stale running graph state", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "delegated_execution_berserker",
      required_role: "builder-berserker",
      execution_ownership: "provider_backed",
      state: "running",
    }];
    const events: TimelineEvent[] = [
      {
        event_id: "evt-start",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "attempt.started",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "delegated_execution_berserker", node_run_id: "nr-builder-1" },
      },
      {
        event_id: "evt-failed",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.failed",
        reason_code: "provider_incomplete_turn",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "delegated_execution_berserker", node_run_id: "nr-builder-1", error: "decode opencode export: unexpected end of JSON input" },
      },
    ];

    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [], lookups);

    expect(milestones).toEqual(expect.arrayContaining([
      expect.objectContaining({
        id: "node-delegated_execution_berserker",
        status: "failed",
        kind: "node_failed",
        detail: "Failed - Provider completion could not be confirmed before the recovery window expired.",
      }),
    ]));
  });

  it("renders recovery-pending executions as running instead of failed", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "execution_review_shieldmaiden",
			node_run_id: "nr-review-1",
			attempt: 1,
			required_role: "reviewer-shieldmaiden",
			execution_ownership: "provider_backed",
			state: "running",
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-review-1",
			node_run_id: "nr-review-1",
			node_id: "execution_review_shieldmaiden",
			attempt: 1,
			agent_id: "agent-reviewer",
			required_role: "reviewer-shieldmaiden",
			execution_mode: "provider_backed",
			status: "verifying",
			queued_at: "2026-03-18T11:41:00Z",
			activity_at: "2026-03-18T11:41:30Z",
			updated_at: "2026-03-18T11:41:30Z",
			terminal_observed_at: "2026-03-18T11:41:30Z",
		}];
		const events: TimelineEvent[] = [{
			event_id: "evt-review-failed",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "attempt.failed",
			reason_code: "provider_incomplete_turn",
			emitted_at: "2026-03-18T11:41:00Z",
			payload_json: { node_id: "execution_review_shieldmaiden", node_run_id: "nr-review-1" },
		}];

		const lookups = buildTimelineLookups(events, nodes, executions);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-execution_review_shieldmaiden",
				status: "running",
				kind: "node_running",
				detail: "Verifying settlement",
			}),
		]));
	});

	it("ignores stale execution activity from an older node lineage", () => {
		const nodes = [{
			node_id: "delegated_execution_berserker",
			required_role: "builder-berserker",
			execution_ownership: "provider_backed",
			state: "running",
			node_run_id: "nr-builder-2",
			attempt: 2,
		}] as Array<RunGraphNode & { node_run_id: string; attempt: number }>;
		const executions: RunExecution[] = [
			{
				attempt_id: "attempt-builder-1",
				node_run_id: "nr-builder-1",
				node_id: "delegated_execution_berserker",
				attempt: 1,
				agent_id: "agent-builder",
				required_role: "builder-berserker",
				execution_mode: "provider_backed",
				status: "failed",
				queued_at: "2026-03-18T11:41:00Z",
				activity_at: "2026-03-18T11:42:00Z",
				updated_at: "2026-03-18T11:42:00Z",
				committed_at: "2026-03-18T11:42:00Z",
				failure_code: "provider_incomplete_turn",
				failure_text: "older failed attempt",
			},
			{
				attempt_id: "attempt-builder-2",
				node_run_id: "nr-builder-2",
				node_id: "delegated_execution_berserker",
				attempt: 2,
				agent_id: "agent-builder",
				required_role: "builder-berserker",
				execution_mode: "provider_backed",
				status: "running",
				queued_at: "2026-03-18T11:41:00Z",
				activity_at: "2026-03-18T11:41:00Z",
				updated_at: "2026-03-18T11:41:00Z",
				started_at: "2026-03-18T11:41:00Z",
			},
		];
		const events: TimelineEvent[] = [{
			event_id: "evt-builder-start-current",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "attempt.started",
			emitted_at: "2026-03-18T11:41:00Z",
			payload_json: { node_id: "delegated_execution_berserker", node_run_id: "nr-builder-2" },
		}];

		const lookups = buildTimelineLookups(events, nodes as RunGraphNode[], executions);
		const milestones = synthesizeMilestones(events, nodes as RunGraphNode[], [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-delegated_execution_berserker",
				status: "running",
				kind: "node_running",
				detail: "In progress",
			}),
		]));
	});

  it("uses friendly failure copy for provider incomplete turn raw events", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "validation_gates_nightraven",
      required_role: "intelligence-nightraven",
      execution_ownership: "provider_backed",
      state: "failed",
    }];
    const event: TimelineEvent = {
      event_id: "evt-incomplete-turn",
      run_event_seq: 1,
      attempt_seq: 1,
      event_type: "attempt.failed",
      reason_code: "provider_incomplete_turn",
      emitted_at: "2026-03-18T11:40:00Z",
      payload_json: { node_id: "validation_gates_nightraven", error: "decode opencode export: unexpected end of JSON input" },
    };

    const lookups = buildTimelineLookups([event], nodes);

    expect(buildRawEventLabel(event, lookups)).toEqual({
      label: "Validation Gates Nightraven failed",
      detail: "Provider completion could not be confirmed before the recovery window expired.",
    });
  });

  it("uses receipt conflict copy for provider incomplete turn conflict raw events", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "delegated_execution_berserker",
      required_role: "builder-berserker",
      execution_ownership: "provider_backed",
      state: "failed",
    }];
    const event: TimelineEvent = {
      event_id: "evt-receipt-conflict",
      run_event_seq: 1,
      attempt_seq: 1,
      event_type: "attempt.failed",
      reason_code: "provider_incomplete_turn",
      emitted_at: "2026-03-18T11:40:00Z",
      payload_json: { node_id: "delegated_execution_berserker", error: "completion_receipt_conflict: conflicting receipt invalidated invocation" },
    };

    const lookups = buildTimelineLookups([event], nodes);

    expect(buildRawEventLabel(event, lookups)).toEqual({
      label: "Delegated Execution Berserker failed",
      detail: "Kingdom detected conflicting completion receipts for this step and could not safely accept the result.",
    });
  });

	it("orders milestones by displayed recency instead of earliest start", () => {
		const nodes: RunGraphNode[] = [
			{
				node_id: "delegated_execution_berserker",
				required_role: "builder-berserker",
				execution_ownership: "provider_backed",
				state: "succeeded",
			},
			{
				node_id: "execution_review_shieldmaiden",
				required_role: "reviewer-shieldmaiden",
				execution_ownership: "provider_backed",
				state: "succeeded",
			},
		];
		const events: TimelineEvent[] = [
			{
				event_id: "evt-builder-start",
				run_event_seq: 1,
				attempt_seq: 1,
				event_type: "attempt.started",
				emitted_at: "2026-03-18T11:40:00Z",
				payload_json: { node_id: "delegated_execution_berserker", node_run_id: "nr-builder-1" },
			},
			{
				event_id: "evt-review-start",
				run_event_seq: 2,
				attempt_seq: 2,
				event_type: "attempt.started",
				emitted_at: "2026-03-18T11:45:00Z",
				payload_json: { node_id: "execution_review_shieldmaiden", node_run_id: "nr-review-1" },
			},
			{
				event_id: "evt-review-success",
				run_event_seq: 3,
				attempt_seq: 3,
				event_type: "attempt.succeeded",
				emitted_at: "2026-03-18T11:50:00Z",
				payload_json: { node_id: "execution_review_shieldmaiden", node_run_id: "nr-review-1" },
			},
			{
				event_id: "evt-builder-success",
				run_event_seq: 4,
				attempt_seq: 4,
				event_type: "attempt.succeeded",
				emitted_at: "2026-03-18T11:55:00Z",
				payload_json: { node_id: "delegated_execution_berserker", node_run_id: "nr-builder-1" },
			},
		];

		const lookups = buildTimelineLookups(events, nodes);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones.map((milestone) => milestone.id)).toEqual([
			"node-execution_review_shieldmaiden",
			"node-delegated_execution_berserker",
		]);
	});

  it("renders a reopened node as queued instead of completed", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "execution_review_shieldmaiden",
      required_role: "reviewer-shieldmaiden",
      execution_ownership: "provider_backed",
      state: "ready",
    }];
    const events: TimelineEvent[] = [
      {
        event_id: "evt-review-success",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "attempt.succeeded",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "execution_review_shieldmaiden", node_run_id: "nr-review-1" },
      },
      {
        event_id: "evt-review-retry",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.retry_queued",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "execution_review_shieldmaiden", node_run_id: "nr-review-2" },
      },
    ];

    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [], lookups);

    expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-execution_review_shieldmaiden",
				status: "ready",
				kind: "node_queued",
				detail: "Queued",
			}),
		]));
	});

  it("renders graph-state pending and blocked nodes without completed copy", () => {
    const nodes: RunGraphNode[] = [
      { node_id: "plan_drafting", required_role: "council-member", execution_ownership: "provider_backed", state: "pending" },
      { node_id: "king_plan_decision", required_role: "king", execution_ownership: "runtime_owned", state: "blocked" },
    ];
    const events: TimelineEvent[] = [
      {
        event_id: "evt-plan-pending",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "node.state_changed",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "plan_drafting", state: "pending" },
      },
      {
        event_id: "evt-king-blocked",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "node.state_changed",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "king_plan_decision", state: "blocked" },
      },
    ];

    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [], lookups);

    expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-plan_drafting",
				status: "pending",
				kind: "node_queued",
				detail: "Pending",
			}),
			expect.objectContaining({
				id: "node-king_plan_decision",
				status: "blocked",
				kind: "node_blocked",
				detail: "Blocked",
			}),
		]));
	});

  it("renders fan-out with queued children as queued instead of completed", () => {
    const nodes: RunGraphNode[] = [
      { node_id: "delegated_execution_berserker", required_role: "builder-berserker", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "validation_gates_qa_scout", required_role: "intelligence-qa-scout", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "validation_gates_nightraven", required_role: "intelligence-nightraven", execution_ownership: "provider_backed", state: "ready" },
    ];
    const events: TimelineEvent[] = [
      {
        event_id: "evt-validation-success",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "attempt.succeeded",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "validation_gates_qa_scout", node_run_id: "nr-validation-qa-1" },
      },
      {
        event_id: "evt-validation-retry",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.retry_queued",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "validation_gates_nightraven", node_run_id: "nr-validation-night-2" },
      },
    ];

    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [
      { from: "delegated_execution_berserker", to: "validation_gates_qa_scout", on: "success" },
      { from: "delegated_execution_berserker", to: "validation_gates_nightraven", on: "success" },
    ], lookups);

    expect(milestones).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: "fan_out",
        status: "ready",
        detail: "1 of 2 succeeded, 1 queued",
      }),
		]));
	});

	it("renders cancelled run milestones with cancelled semantics", () => {
		const events: TimelineEvent[] = [{
			event_id: "evt-run-cancelled",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "run.state_changed",
			reason_code: "operator_cancelled",
			emitted_at: "2026-03-18T11:40:00Z",
			payload_json: { action: "cancel", status: "cancelled" },
		}];

		const lookups = buildTimelineLookups(events, []);
		const milestones = synthesizeMilestones(events, [], [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				label: "Run cancelled",
				status: "cancelled",
			}),
		]));
	});
});
