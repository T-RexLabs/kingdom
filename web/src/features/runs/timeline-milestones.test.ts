import { describe, expect, it } from "vitest";
import type { RunExecution, RunGraphNode, TimelineEvent } from "@/api/hooks-types";
import { buildRawEventLabel, buildTimelineLookups, synthesizeMilestones } from "./timeline-milestones";

describe("timeline milestones", () => {
  it.each([
    ["attempt.progress", "runtime_completion_observation", "QA Review Completion Observed"],
    ["attempt.failed", "execution_review_rework", "QA Review Requested Rework"],
    ["attempt.retry_queued", "", "QA Review Retry Queued"],
  ])("uses title case for %s without changing role acronyms or event content", (eventType, reasonCode, label) => {
    const event: TimelineEvent = {
      event_id: "evt-title",
      run_event_seq: 1,
      attempt_seq: 1,
      event_type: eventType,
      reason_code: reasonCode,
      emitted_at: "2026-03-18T11:40:00Z",
      payload_json: { node_id: "qa_review", note: "Leave this sentence alone." },
    };
    const before = JSON.stringify(event);
    expect(buildRawEventLabel(event, buildTimelineLookups([event], [])).label).toBe(label);
    expect(JSON.stringify(event)).toBe(before);
  });

	it("labels a blocked recovery and shows what Kingdom is waiting on", () => {
		const event: TimelineEvent = {
			event_id: "evt-recovery-blocked",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "attempt.recovery_blocked",
			reason_code: "recovery_blocked",
			emitted_at: "2026-03-18T11:40:00Z",
			payload_json: {
				node_id: "qa_review",
				operator_text: "A file change was in flight and the provider server making it is not yet proven stopped, so Kingdom cannot yet tell what it changed.",
			},
		};
		expect(buildRawEventLabel(event, buildTimelineLookups([event], []))).toEqual({
			label: "QA Review Recovery Blocked",
			detail: "A file change was in flight and the provider server making it is not yet proven stopped, so Kingdom cannot yet tell what it changed.",
		});
	});

	it.each([
		["run.retry_reopened", "operator_retry", "Manual Retry Scheduled - Attempt 1"],
		["run.rerun_reopened", "operator_rerun", "Manual Rerun Scheduled - Attempt 1"],
	] as const)("creates one recovery milestone for %s", (eventType, trigger, label) => {
		const event: TimelineEvent = {
			event_id: `evt-${eventType}`,
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: eventType,
			reason_code: eventType === "run.retry_reopened" ? "manual_retry_requested" : "manual_rerun_requested",
			emitted_at: "2026-03-18T11:40:00Z",
			payload_json: {
				recovery_trigger: trigger,
				recovery_phase: "scheduled",
				error_code: eventType === "run.retry_reopened" ? "manual_retry_requested" : "manual_rerun_requested",
				retry_index: 0,
				retry_max: 0,
				scheduled_at: "2026-03-18T11:40:00Z",
			},
		};

		const milestones = synthesizeMilestones([event], [], [], buildTimelineLookups([event], []));

		expect(milestones).toEqual([
			expect.objectContaining({ id: `recovery-${event.event_id}`, label, status: "ready" }),
		]);
	});

	it.each([
		["retry", "run.retry_reopened", "attempt.retry_queued", "operator_retry", "manual_retry_requested", "Manual Retry Scheduled - Attempt 1"],
		["rerun", "run.rerun_reopened", "attempt.rerun_queued", "operator_rerun", "manual_rerun_requested", "Manual Rerun Scheduled - Attempt 1"],
	] as const)("deduplicates producer-shaped manual %s recovery events", (_action, runEventType, attemptEventType, trigger, code, label) => {
		const scheduledAt = "2026-03-18T11:40:00Z";
		const runEvent: TimelineEvent = {
			event_id: `evt-${runEventType}`,
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: runEventType,
			reason_code: code,
			emitted_at: scheduledAt,
			payload_json: {
				node_id: "implementation",
				node_run_id: "nr-builder-source",
				recovery_trigger: trigger,
				recovery_phase: "scheduled",
				error_code: code,
				retry_index: 0,
				retry_max: 0,
				scheduled_at: scheduledAt,
			},
		};
		const attemptEvent: TimelineEvent = {
			event_id: `evt-${attemptEventType}`,
			run_event_seq: 2,
			attempt_seq: 1,
			event_type: attemptEventType,
			reason_code: code,
			emitted_at: scheduledAt,
			payload_json: {
				node_id: "implementation",
				node_run_id: "nr-builder-target",
				recovery_trigger: trigger,
				recovery_phase: "scheduled",
				error_code: code,
				source_attempt_id: "run-1:implementation:1",
				source_node_run_id: "nr-builder-source",
				target_attempt_id: "run-1:implementation:2",
				target_node_run_id: "nr-builder-target",
				retry_index: 0,
				retry_max: 0,
				scheduled_at: scheduledAt,
			},
		};

		const events = [runEvent, attemptEvent];
		const milestones = synthesizeMilestones(events, [], [], buildTimelineLookups(events, []));
		const recoveryMilestones = milestones.filter((milestone) => milestone.id.startsWith("recovery-"));

		expect(recoveryMilestones).toEqual([
			expect.objectContaining({
				id: `recovery-${attemptEvent.event_id}`,
				label,
				detail: "Implementation",
				status: "ready",
				events: [runEvent, attemptEvent],
			}),
		]);
	});

	it("shows node detail from a producer-shaped recovery payload", () => {
		const events: TimelineEvent[] = [{
			event_id: "retry-scheduled", run_event_seq: 1, attempt_seq: 1, event_type: "attempt.retry_scheduled", reason_code: "llm_rate_limited", emitted_at: "2026-03-18T11:40:00Z",
			payload_json: {
				node_id: "implementation",
				node_run_id: "nr-builder-source",
				retry_decision_id: "retry-builder-1",
				decision: "retry",
				recovery_trigger: "automatic_retry",
				recovery_phase: "scheduled",
				error_code: "llm_rate_limited",
				source_attempt_id: "run-1:implementation:1",
				source_node_run_id: "nr-builder-source",
				target_attempt_id: "run-1:implementation:2",
				target_node_run_id: "nr-builder-target",
				retry_index: 1,
				retry_max: 3,
				scheduled_at: "2026-03-18T11:41:00Z",
			},
		}];
		const nodes: RunGraphNode[] = [{ node_id: "implementation", required_role: "builder", execution_ownership: "provider_backed", state: "blocked" }];
		const lookups = buildTimelineLookups(events, nodes);
		expect(buildRawEventLabel(events[0], lookups)).toEqual({ label: "Retry Scheduled - Rate Limit", detail: "Implementation" });
		expect(synthesizeMilestones(events, nodes, [], lookups)).toEqual(expect.arrayContaining([expect.objectContaining({ label: "Retry Scheduled - Rate Limit", status: "ready" })]));
	});

	it("presents final governance rework separately from Manual Rerun", () => {
		const event: TimelineEvent = {
			event_id: "final-rework-queued",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "attempt.final_rework_queued",
			reason_code: "rework_requested",
			emitted_at: "2026-03-18T11:40:00Z",
			payload_json: {
				node_id: "plan_drafting",
				node_run_id: "nr-plan-rework",
				attempt_id: "run-1:plan_drafting:2",
				attempt: 2,
				governance_rework: "final",
				state: "ready",
			},
		};
		const nodes: RunGraphNode[] = [{
			node_id: "plan_drafting",
			required_role: "plan-author",
			execution_ownership: "provider_backed",
			state: "ready",
		}];
		const lookups = buildTimelineLookups([event], nodes);
		const milestones = synthesizeMilestones([event], nodes, [], lookups);

		expect(buildRawEventLabel(event, lookups)).toEqual({ label: "Plan Drafting Final Governance Rework Queued" });
		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({ label: "Plan Drafting", detail: "Final Governance Rework Queued", status: "ready" }),
		]));
		expect(JSON.stringify(milestones)).not.toContain("Manual Rerun");
	});

	it("describes runtime-owned raw events without default execution metadata", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "plan_decision",
      required_role: "governance",
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
        node_id: "plan_decision",
        communication_type: "approval_request",
        visible_to_operator: true,
      },
    }];

    const lookups = buildTimelineLookups(events, nodes);

		expect(buildRawEventLabel(events[0], lookups)).toEqual({
			label: "Plan Decision Awaiting Approval",
			detail: undefined,
		});
	});

	it("keeps provider-backed node milestones focused on outcome", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "implementation",
      required_role: "builder",
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
        payload_json: { node_id: "implementation" },
      },
      {
        event_id: "evt-success",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.succeeded",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "implementation" },
      },
    ];

    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-implementation",
				label: "Implementation",
				detail: "Succeeded",
			}),
		]));
	});

	it("labels retry and progress attempt events with runtime semantics instead of generic fallbacks", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "implementation",
			required_role: "builder",
			execution_ownership: "provider_backed",
			state: "running",
		}];
		const retryQueued: TimelineEvent = {
			event_id: "evt-retry-queued",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "attempt.retry_queued",
			emitted_at: "2026-03-18T11:40:00Z",
			payload_json: { node_id: "implementation" },
		};
		const progress: TimelineEvent = {
			event_id: "evt-progress",
			run_event_seq: 2,
			attempt_seq: 2,
			event_type: "attempt.progress",
			reason_code: "provider_invoked",
			emitted_at: "2026-03-18T11:41:00Z",
			payload_json: { node_id: "implementation" },
		};
		const provisionalText: TimelineEvent = {
			event_id: "evt-provisional-text",
			run_event_seq: 3,
			attempt_seq: 3,
			event_type: "attempt.progress",
			reason_code: "stream_event",
			emitted_at: "2026-03-18T11:42:00Z",
			payload_json: { node_id: "implementation", text: "Partial response" },
		};

		const lookups = buildTimelineLookups([retryQueued, progress, provisionalText], nodes);

		expect(buildRawEventLabel(retryQueued, lookups)).toEqual({
			label: "Implementation Retry Queued",
			detail: undefined,
		});
		expect(buildRawEventLabel(progress, lookups)).toEqual({
			label: "Implementation Provider Running",
			detail: undefined,
		});
		expect(buildRawEventLabel(provisionalText, lookups)).toEqual({
			label: "Implementation Provisional Stream Text",
			detail: "Not Canonical",
		});
	});

	it("renders blocked recovery labels and milestones from live execution state", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "implementation",
			node_run_id: "nr-builder-1",
			required_role: "builder",
			execution_ownership: "provider_backed",
			state: "running",
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-builder-1",
			node_run_id: "nr-builder-1",
			node_id: "implementation",
			attempt: 1,
			agent_id: "agent-builder",
			required_role: "builder",
			execution_mode: "provider_backed",
			status: "blocked",
			queued_at: "2026-03-18T11:39:30Z",
			activity_at: "2026-03-18T11:41:00Z",
			updated_at: "2026-03-18T11:41:00Z",
			stop_retry_available: false,
			launch_started_at: "2026-03-18T11:39:30Z",
		}];
		const recovered: TimelineEvent = {
			event_id: "evt-recovered-blocked",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "node.state_changed",
			emitted_at: "2026-03-18T11:41:00Z",
			payload_json: { node_id: "implementation", node_run_id: "nr-builder-1", state: "blocked" },
		};

		const lookups = buildTimelineLookups([recovered], nodes, executions);
		expect(buildRawEventLabel(recovered, lookups)).toEqual({
			label: "Implementation Blocked",
			detail: undefined,
		});

		const milestones = synthesizeMilestones([recovered], nodes, [], lookups);
		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-implementation",
				status: "blocked",
				kind: "node_blocked",
				detail: "Blocked",
			}),
		]));
	});

	it("keeps a quiescence-blocked retry visibly paused in the timeline", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "implementation",
			node_run_id: "nr-builder-paused",
			required_role: "builder",
			execution_ownership: "provider_backed",
			state: "blocked",
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-builder-paused",
			node_run_id: "nr-builder-paused",
			node_id: "implementation",
			attempt: 1,
			agent_id: "agent-builder",
			required_role: "builder",
			execution_mode: "provider_backed",
			status: "paused",
			queued_at: "2026-03-18T11:39:30Z",
			activity_at: "2026-03-18T11:41:00Z",
			updated_at: "2026-03-18T11:41:00Z",
			stop_retry_available: false,
			launch_started_at: "2026-03-18T11:39:30Z",
		}];

		const recoveryEvent: TimelineEvent = {
			event_id: "evt-retry-paused",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "node.state_changed",
			emitted_at: "2026-03-18T11:41:00Z",
			payload_json: { node_id: "implementation", node_run_id: "nr-builder-paused", state: "blocked" },
		};
		const retryEvent: TimelineEvent = {
			event_id: "evt-retry-queued",
			run_event_seq: 2,
			attempt_seq: 1,
			event_type: "attempt.retry_queued",
			emitted_at: "2026-03-18T11:42:00Z",
			payload_json: { node_id: "implementation", node_run_id: "nr-builder-paused-2", attempt: 2 },
		};
		const milestones = synthesizeMilestones([recoveryEvent, retryEvent], nodes, [], buildTimelineLookups([recoveryEvent, retryEvent], nodes, executions));
		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-implementation",
				status: "paused",
				kind: "node_blocked",
				detail: "Retry Paused - Previous Attempt Still Running",
				events: expect.arrayContaining([expect.objectContaining({ event_id: "evt-retry-queued" })]),
			}),
		]));
	});

	it("describes a paused workspace-restoration handoff accurately", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "implementation",
			node_run_id: "nr-builder-restore",
			required_role: "builder",
			execution_ownership: "provider_backed",
			state: "blocked",
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-builder-restore",
			node_run_id: "nr-builder-restore",
			node_id: "implementation",
			attempt: 1,
			agent_id: "agent-builder",
			required_role: "builder",
			execution_mode: "provider_backed",
			status: "paused",
			handoff_state: "workspace_restore_failed",
			queued_at: "2026-03-18T11:39:30Z",
			activity_at: "2026-03-18T11:41:00Z",
			updated_at: "2026-03-18T11:41:00Z",
			stop_retry_available: false,
			launch_started_at: "2026-03-18T11:39:30Z",
		}];
		const recoveryEvent: TimelineEvent = {
			event_id: "evt-retry-restore-paused",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "node.state_changed",
			emitted_at: "2026-03-18T11:41:00Z",
			payload_json: { node_id: "implementation", node_run_id: "nr-builder-restore", state: "blocked" },
		};

		const milestones = synthesizeMilestones([recoveryEvent], nodes, [], buildTimelineLookups([recoveryEvent], nodes, executions));
		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-implementation",
				status: "paused",
				detail: "Step Paused - Trusted Workspace Restoration Failed",
			}),
		]));
	});

	it("keeps blocked recovery visible while still showing the queued retry handoff", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "implementation",
			node_run_id: "nr-builder-1",
			required_role: "builder",
			execution_ownership: "provider_backed",
			state: "blocked",
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-builder-1",
			node_run_id: "nr-builder-1",
			node_id: "implementation",
			attempt: 1,
			agent_id: "agent-builder",
			required_role: "builder",
			execution_mode: "provider_backed",
			status: "blocked",
			queued_at: "2026-03-18T11:39:30Z",
			activity_at: "2026-03-18T11:42:00Z",
			updated_at: "2026-03-18T11:42:00Z",
			stop_retry_available: false,
			launch_started_at: "2026-03-18T11:39:30Z",
		}];
		const events: TimelineEvent[] = [
			{
				event_id: "evt-recovered-blocked",
				run_event_seq: 5,
				attempt_seq: 3,
				event_type: "node.state_changed",
				emitted_at: "2026-03-18T11:41:00Z",
				payload_json: { node_id: "implementation", node_run_id: "nr-builder-1", state: "blocked" },
			},
			{
				event_id: "evt-retry-queued",
				run_event_seq: 6,
				attempt_seq: 1,
				event_type: "attempt.retry_queued",
				emitted_at: "2026-03-18T11:42:00Z",
				payload_json: { node_id: "implementation", node_run_id: "nr-builder-2", attempt: 2 },
			},
		];

		const lookups = buildTimelineLookups(events, nodes, executions);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-implementation",
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
			node_id: "implementation",
			node_run_id: "nr-builder-1",
			required_role: "builder",
			execution_ownership: "provider_backed",
			state: "running",
		}];
		const events: TimelineEvent[] = [
			{
				event_id: "evt-recovered-blocked-live-only",
				run_event_seq: 5,
				attempt_seq: 3,
				event_type: "node.state_changed",
				emitted_at: "2026-03-18T11:41:00Z",
				payload_json: { node_id: "implementation", node_run_id: "nr-builder-1", state: "blocked" },
			},
			{
				event_id: "evt-retry-queued-live-only",
				run_event_seq: 6,
				attempt_seq: 1,
				event_type: "attempt.retry_queued",
				emitted_at: "2026-03-18T11:42:00Z",
				payload_json: { node_id: "implementation", node_run_id: "nr-builder-2", attempt: 2 },
			},
		];

		const lookups = buildTimelineLookups(events, nodes, []);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-implementation",
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
			node_id: "systems_consultation",
			node_run_id: "nr-burgher-1",
			required_role: "systems-advisor",
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
			payload_json: { node_id: "systems_consultation", node_run_id: "nr-burgher-old" },
		}, {
			event_id: "evt-burgher-ready",
			run_event_seq: 2,
			attempt_seq: 1,
			event_type: "node.state_changed",
			emitted_at: "2026-03-18T11:39:00Z",
			payload_json: { node_id: "systems_consultation", node_run_id: "nr-burgher-1", state: "ready" },
		}, {
			event_id: "evt-burgher-starting",
			run_event_seq: 3,
			attempt_seq: 2,
			event_type: "node.state_changed",
			emitted_at: "2026-03-18T11:39:30Z",
			payload_json: { node_id: "systems_consultation", node_run_id: "nr-burgher-1", state: "starting" },
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-burgher-1",
			node_run_id: "nr-burgher-1",
			node_id: "systems_consultation",
			attempt: 1,
			agent_id: "agent-burgher",
			required_role: "systems-advisor",
			execution_mode: "provider_backed",
			status: "starting",
			queued_at: "2026-03-18T11:39:00Z",
			activity_at: "2026-03-18T11:41:00Z",
			updated_at: "2026-03-18T11:41:00Z",
			stop_retry_available: false,
			launch_started_at: "2026-03-18T11:39:30Z",
			started_at: "2026-03-18T11:40:15Z",
		}];

		const lookups = buildTimelineLookups(events, nodes, executions);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-systems_consultation",
				status: "starting",
				detail: "Launching",
				timestamp: "2026-03-18T11:39:30Z",
			}),
		]));
	});

	it("anchors running milestones to provider invocation instead of launch start", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "systems_consultation",
			node_run_id: "nr-burgher-running",
			required_role: "systems-advisor",
			execution_ownership: "provider_backed",
			state: "running",
		}];
		const events: TimelineEvent[] = [{
			event_id: "evt-burgher-starting",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "node.state_changed",
			emitted_at: "2026-03-18T11:39:30Z",
			payload_json: { node_id: "systems_consultation", node_run_id: "nr-burgher-running", state: "starting" },
		}, {
			event_id: "evt-burgher-invoked",
			run_event_seq: 2,
			attempt_seq: 2,
			event_type: "attempt.progress",
			reason_code: "provider_invoked",
			emitted_at: "2026-03-18T11:40:15Z",
			payload_json: { node_id: "systems_consultation", node_run_id: "nr-burgher-running" },
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-burgher-running",
			node_run_id: "nr-burgher-running",
			node_id: "systems_consultation",
			attempt: 1,
			agent_id: "agent-burgher",
			required_role: "systems-advisor",
			execution_mode: "provider_backed",
			status: "running",
			queued_at: "2026-03-18T11:39:30Z",
			activity_at: "2026-03-18T11:41:00Z",
			updated_at: "2026-03-18T11:41:00Z",
			stop_retry_available: false,
			launch_started_at: "2026-03-18T11:39:30Z",
			progress_at: "2026-03-18T11:40:15Z",
		}];

		const lookups = buildTimelineLookups(events, nodes, executions);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-systems_consultation",
				status: "running",
				timestamp: "2026-03-18T11:40:15Z",
			}),
		]));
	});

  it("labels review fan-out cap auto-success with the review badge", () => {
    const nodes: RunGraphNode[] = [
      { node_id: "implementation", required_role: "builder", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "execution_review_a", required_role: "reviewer", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "execution_review_b", required_role: "reviewer", execution_ownership: "provider_backed", state: "succeeded" },
    ];
    const events: TimelineEvent[] = [
      {
        event_id: "evt-review-a",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "attempt.succeeded",
        reason_code: "execution_review_cap_auto_succeeded",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "execution_review_a", node_run_id: "nr-review-a", status: "ok" },
      },
      {
        event_id: "evt-review-b",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.succeeded",
        reason_code: "execution_completed",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "execution_review_b", node_run_id: "nr-review-b", status: "ok" },
      },
    ];
    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [
      { from: "implementation", to: "execution_review_a", on: "success" },
      { from: "implementation", to: "execution_review_b", on: "success" },
    ], lookups);

    expect(milestones.find((milestone) => milestone.kind === "fan_out")?.badge).toBe("Review Cap Auto-Succeeded");
  });

  it("uses a generic badge when fan-out mixes review and validation cap auto-success", () => {
    const nodes: RunGraphNode[] = [
      { node_id: "implementation", required_role: "builder", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "execution_review", required_role: "reviewer", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "quality_validation", required_role: "qa-validator", execution_ownership: "provider_backed", state: "succeeded" },
    ];
    const events: TimelineEvent[] = [
      {
        event_id: "evt-review",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "attempt.succeeded",
        reason_code: "execution_review_cap_auto_succeeded",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "execution_review", node_run_id: "nr-review", status: "ok" },
      },
      {
        event_id: "evt-validation",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.succeeded",
        reason_code: "validation_gate_cap_auto_succeeded",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "quality_validation", node_run_id: "nr-validation", status: "ok" },
      },
    ];
    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [
      { from: "implementation", to: "execution_review", on: "success" },
      { from: "implementation", to: "quality_validation", on: "success" },
    ], lookups);

    expect(milestones.find((milestone) => milestone.kind === "fan_out")?.badge).toBe("Cap Auto-Succeeded");
  });

  it("shows a node as running when a later attempt starts after an older success", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "security_validation",
      required_role: "security-validator",
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
        payload_json: { node_id: "security_validation" },
      },
      {
        event_id: "evt-start-new",
        run_event_seq: 2,
        attempt_seq: 1,
        event_type: "attempt.started",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "security_validation" },
      },
    ];

    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-security_validation",
				status: "running",
				kind: "node_running",
				detail: "In progress",
			}),
		]));
	});

  it("prefers the latest streamed terminal status over a stale running graph state", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "implementation",
      required_role: "builder",
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
        payload_json: { node_id: "implementation", node_run_id: "nr-builder-1" },
      },
      {
        event_id: "evt-failed",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.failed",
        reason_code: "provider_incomplete_turn",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "implementation", node_run_id: "nr-builder-1", error: "decode opencode export: unexpected end of JSON input" },
      },
    ];

    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-implementation",
				status: "failed",
				kind: "node_failed",
				detail: "Failed - Kingdom could not confirm a clean finish for this step.",
			}),
		]));
  });

  it("renders recovery-pending executions as running instead of failed", () => {
		const nodes: RunGraphNode[] = [{
			node_id: "execution_review",
			node_run_id: "nr-review-1",
			attempt: 1,
			required_role: "reviewer",
			execution_ownership: "provider_backed",
			state: "running",
		}];
		const executions: RunExecution[] = [{
			attempt_id: "attempt-review-1",
			node_run_id: "nr-review-1",
			node_id: "execution_review",
			attempt: 1,
			agent_id: "agent-reviewer",
			required_role: "reviewer",
			execution_mode: "provider_backed",
			status: "verifying",
			queued_at: "2026-03-18T11:41:00Z",
			activity_at: "2026-03-18T11:41:30Z",
			updated_at: "2026-03-18T11:41:30Z",
			stop_retry_available: false,
			terminal_observed_at: "2026-03-18T11:41:30Z",
		}];
		const events: TimelineEvent[] = [{
			event_id: "evt-review-failed",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "attempt.failed",
			reason_code: "provider_incomplete_turn",
			emitted_at: "2026-03-18T11:41:00Z",
			payload_json: { node_id: "execution_review", node_run_id: "nr-review-1" },
		}];

		const lookups = buildTimelineLookups(events, nodes, executions);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-execution_review",
				status: "running",
				kind: "node_running",
				detail: "Verifying settlement",
			}),
		]));
	});

	it("ignores stale execution activity from an older node lineage", () => {
		const nodes = [{
			node_id: "implementation",
			required_role: "builder",
			execution_ownership: "provider_backed",
			state: "running",
			node_run_id: "nr-builder-2",
			attempt: 2,
		}] as Array<RunGraphNode & { node_run_id: string; attempt: number }>;
		const executions: RunExecution[] = [
			{
				attempt_id: "attempt-builder-1",
				node_run_id: "nr-builder-1",
				node_id: "implementation",
				attempt: 1,
				agent_id: "agent-builder",
				required_role: "builder",
				execution_mode: "provider_backed",
				status: "failed",
				queued_at: "2026-03-18T11:41:00Z",
				activity_at: "2026-03-18T11:42:00Z",
				updated_at: "2026-03-18T11:42:00Z",
				stop_retry_available: false,
				committed_at: "2026-03-18T11:42:00Z",
				failure_code: "provider_incomplete_turn",
				failure_text: "older failed attempt",
			},
			{
				attempt_id: "attempt-builder-2",
				node_run_id: "nr-builder-2",
				node_id: "implementation",
				attempt: 2,
				agent_id: "agent-builder",
				required_role: "builder",
				execution_mode: "provider_backed",
				status: "running",
				queued_at: "2026-03-18T11:41:00Z",
				activity_at: "2026-03-18T11:41:00Z",
				updated_at: "2026-03-18T11:41:00Z",
				stop_retry_available: false,
				started_at: "2026-03-18T11:41:00Z",
			},
		];
		const events: TimelineEvent[] = [{
			event_id: "evt-builder-start-current",
			run_event_seq: 1,
			attempt_seq: 1,
			event_type: "attempt.started",
			emitted_at: "2026-03-18T11:41:00Z",
			payload_json: { node_id: "implementation", node_run_id: "nr-builder-2" },
		}];

		const lookups = buildTimelineLookups(events, nodes as RunGraphNode[], executions);
		const milestones = synthesizeMilestones(events, nodes as RunGraphNode[], [], lookups);

		expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-implementation",
				status: "running",
				kind: "node_running",
				detail: "In progress",
			}),
		]));
	});

  it("uses friendly failure copy for provider incomplete turn raw events", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "security_validation",
      required_role: "security-validator",
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
      payload_json: { node_id: "security_validation", error: "decode opencode export: unexpected end of JSON input" },
    };

    const lookups = buildTimelineLookups([event], nodes);

		expect(buildRawEventLabel(event, lookups)).toEqual({
		  label: "Security Validation Failed",
		  detail: "Kingdom could not confirm a clean finish for this step.",
		});
  });

	it("orders milestones by displayed recency instead of earliest start", () => {
		const nodes: RunGraphNode[] = [
			{
				node_id: "implementation",
				required_role: "builder",
				execution_ownership: "provider_backed",
				state: "succeeded",
			},
			{
				node_id: "execution_review",
				required_role: "reviewer",
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
				payload_json: { node_id: "implementation", node_run_id: "nr-builder-1" },
			},
			{
				event_id: "evt-review-start",
				run_event_seq: 2,
				attempt_seq: 2,
				event_type: "attempt.started",
				emitted_at: "2026-03-18T11:45:00Z",
				payload_json: { node_id: "execution_review", node_run_id: "nr-review-1" },
			},
			{
				event_id: "evt-review-success",
				run_event_seq: 3,
				attempt_seq: 3,
				event_type: "attempt.succeeded",
				emitted_at: "2026-03-18T11:50:00Z",
				payload_json: { node_id: "execution_review", node_run_id: "nr-review-1" },
			},
			{
				event_id: "evt-builder-success",
				run_event_seq: 4,
				attempt_seq: 4,
				event_type: "attempt.succeeded",
				emitted_at: "2026-03-18T11:55:00Z",
				payload_json: { node_id: "implementation", node_run_id: "nr-builder-1" },
			},
		];

		const lookups = buildTimelineLookups(events, nodes);
		const milestones = synthesizeMilestones(events, nodes, [], lookups);

		expect(milestones.map((milestone) => milestone.id)).toEqual([
			"node-execution_review",
			"node-implementation",
		]);
	});

  it("renders a reopened node as queued instead of completed", () => {
    const nodes: RunGraphNode[] = [{
      node_id: "execution_review",
      required_role: "reviewer",
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
        payload_json: { node_id: "execution_review", node_run_id: "nr-review-1" },
      },
      {
        event_id: "evt-review-retry",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.retry_queued",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "execution_review", node_run_id: "nr-review-2" },
      },
    ];

    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [], lookups);

    expect(milestones).toEqual(expect.arrayContaining([
			expect.objectContaining({
				id: "node-execution_review",
				status: "ready",
				kind: "node_queued",
				detail: "Queued",
			}),
		]));
	});

  it("renders graph-state pending and blocked nodes without completed copy", () => {
    const nodes: RunGraphNode[] = [
      { node_id: "plan_drafting", required_role: "plan-author", execution_ownership: "provider_backed", state: "pending" },
      { node_id: "plan_decision", required_role: "governance", execution_ownership: "runtime_owned", state: "blocked" },
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
        event_id: "evt-governance-blocked",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "node.state_changed",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "plan_decision", state: "blocked" },
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
				id: "node-plan_decision",
				status: "blocked",
				kind: "node_blocked",
				detail: "Blocked",
			}),
		]));
	});

  it("renders fan-out with queued children as queued instead of completed", () => {
    const nodes: RunGraphNode[] = [
      { node_id: "implementation", required_role: "builder", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "quality_validation", required_role: "qa-validator", execution_ownership: "provider_backed", state: "succeeded" },
      { node_id: "security_validation", required_role: "security-validator", execution_ownership: "provider_backed", state: "ready" },
    ];
    const events: TimelineEvent[] = [
      {
        event_id: "evt-validation-success",
        run_event_seq: 1,
        attempt_seq: 1,
        event_type: "attempt.succeeded",
        emitted_at: "2026-03-18T11:40:00Z",
        payload_json: { node_id: "quality_validation", node_run_id: "nr-validation-qa-1" },
      },
      {
        event_id: "evt-validation-retry",
        run_event_seq: 2,
        attempt_seq: 2,
        event_type: "attempt.retry_queued",
        emitted_at: "2026-03-18T11:41:00Z",
        payload_json: { node_id: "security_validation", node_run_id: "nr-validation-night-2" },
      },
    ];

    const lookups = buildTimelineLookups(events, nodes);
    const milestones = synthesizeMilestones(events, nodes, [
      { from: "implementation", to: "quality_validation", on: "success" },
      { from: "implementation", to: "security_validation", on: "success" },
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
				label: "Run Cancelled",
				status: "cancelled",
			}),
		]));
	});
});
