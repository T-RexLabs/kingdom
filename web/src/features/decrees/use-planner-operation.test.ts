import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";
import type {
  PlanningOperationDetail,
  PlanningOperationEvent,
  PlanningSessionDetail,
  PlanningSessionListResponse,
} from "@/api/hooks";
import { plannerKeys } from "./planner-query-keys";
import { patchPlannerTerminalCaches, reconcileTerminalOperationSnapshot } from "./use-planner-operation";

function buildSession(overrides: Partial<PlanningSessionDetail> = {}): PlanningSessionDetail {
  return {
    planning_session_id: "ps_1",
    title: "Planner kickoff",
    title_state: "custom",
    lifecycle_state: "active",
    conversation_state: "processing",
    current_question_message_id: "msg_question",
    current_operation_id: "op_running",
    current_title_operation_id: undefined,
    current_draft_id: undefined,
    context_snapshot_id: "snap_1",
    created_at: "2026-04-21T10:00:00Z",
    updated_at: "2026-04-21T10:05:00Z",
    project_root: "/tmp/project",
    model_selection: { model: "github-copilot/gpt-5.4" },
    ...overrides,
  };
}

function buildOp(overrides: Partial<PlanningOperationDetail> = {}): PlanningOperationDetail {
  return {
    operation_id: "op_running",
    planning_session_id: "ps_1",
    type: "reply",
    status: "running",
    request: {},
    error: null,
    result_ref: null,
    observed_output_text: undefined,
    created_at: "2026-04-21T10:00:00Z",
    finished_at: null,
    ...overrides,
  } as PlanningOperationDetail;
}

function buildEvent(
  type: PlanningOperationEvent["event_type"],
  payload: PlanningOperationEvent["payload"] = {},
): PlanningOperationEvent {
  return {
    event_id: "op_running:1",
    operation_id: "op_running",
    seq: 1,
    event_type: type,
    payload,
    created_at: "2026-04-21T10:05:00Z",
  };
}

describe("patchPlannerTerminalCaches — field-scoped reconciliation", () => {
  it("clears conversation_state and current_operation_id on a done event without touching detail-only fields", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), buildSession());

    patchPlannerTerminalCaches(qc, buildOp(), buildEvent("done", { planning_message_id: "msg_new_question" }));

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(detail?.conversation_state).toBe("ready");
    expect(detail?.current_operation_id).toBeUndefined();
    expect(detail?.current_question_message_id).toBe("msg_new_question");
    // Detail-only fields untouched.
    expect(detail?.model_selection).toEqual({ model: "github-copilot/gpt-5.4" });
    expect(detail?.project_root).toBe("/tmp/project");
  });

  it("applies authoritative session patches carried by reply terminal events", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), buildSession());

    patchPlannerTerminalCaches(
      qc,
      buildOp(),
      buildEvent("done", {
        planning_message_id: "msg_new_question",
        session_patch: {
          current_title_operation_id: "op_title_auto",
          updated_at: "2026-04-21T10:06:00Z",
        },
      }),
    );

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(detail?.current_title_operation_id).toBe("op_title_auto");
    expect(detail?.updated_at).toBe("2026-04-21T10:06:00Z");
    expect(detail?.current_question_message_id).toBe("msg_new_question");
  });

  it("preserves concurrent SSE updates to non-terminal fields", () => {
    // Regression guard for the cross-slice clobber class. A separate
    // SSE event (or a model autosave onSuccess) may have just
    // updated context_snapshot_id between when the terminal event
    // was queued and when patchPlannerTerminalCaches runs.
    // Field-scoped reconciliation only writes the terminal slice;
    // unrelated fields survive.
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(
      plannerKeys.detail("ps_1"),
      buildSession({ context_snapshot_id: "snap_just_refreshed" }),
    );

    patchPlannerTerminalCaches(qc, buildOp(), buildEvent("done", { planning_message_id: "msg_new" }));

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(detail?.context_snapshot_id).toBe("snap_just_refreshed");
    expect(detail?.current_operation_id).toBeUndefined();
  });

  it("sets current_draft_id on a formalize done event", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), buildSession());

    patchPlannerTerminalCaches(
      qc,
      buildOp({ type: "formalize" }),
      buildEvent("done", { planning_draft_id: "draft_1" }),
    );

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(detail?.current_draft_id).toBe("draft_1");
    expect(detail?.current_operation_id).toBeUndefined();
    expect(detail?.conversation_state).toBe("ready");
  });

  it("preserves a newer custom title when generate_title completes", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(
      plannerKeys.detail("ps_1"),
      buildSession({
        title: "Operator title",
        title_state: "custom",
        current_operation_id: "op_interactive",
        current_title_operation_id: "op_running",
      }),
    );
    qc.setQueryData<PlanningSessionListResponse>(plannerKeys.list(50), {
      items: [
        {
          planning_session_id: "ps_1",
          title: "Operator title",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "processing",
          current_question_message_id: "msg_question",
          current_operation_id: "op_interactive",
          current_title_operation_id: "op_running",
          current_draft_id: undefined,
          context_snapshot_id: "snap_1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
        },
      ],
    });

    patchPlannerTerminalCaches(
      qc,
      buildOp({ type: "generate_title" }),
      buildEvent("done", { title: "Generated title" }),
    );

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(detail?.title).toBe("Operator title");
    expect(detail?.title_state).toBe("custom");
    expect(detail?.conversation_state).toBe("processing");
    expect(detail?.current_operation_id).toBe("op_interactive");
    expect(detail?.current_title_operation_id).toBeUndefined();

    const item = qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list(50))?.items[0];
    expect(item?.title).toBe("Operator title");
    expect(item?.title_state).toBe("custom");
    expect(item?.conversation_state).toBe("processing");
    expect(item?.current_operation_id).toBe("op_interactive");
    expect(item?.current_title_operation_id).toBeUndefined();
  });

  it("treats cancelled the same way as error for cache reset", () => {
    // Both clear conversation_state + current_operation_id without
    // setting current_question_message_id (no message was produced).
    // The status difference is encoded in the local
    // PlannerOperationState reducer, not the session-detail cache.
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), buildSession());

    patchPlannerTerminalCaches(qc, buildOp(), buildEvent("cancelled", { reason: "shutdown" }));

    const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
    expect(detail?.conversation_state).toBe("ready");
    expect(detail?.current_operation_id).toBeUndefined();
    // current_question_message_id only updates on a `done` event with
    // a planning_message_id payload; cancelled doesn't carry one.
    expect(detail?.current_question_message_id).toBe("msg_question");
  });

  it("ignores non-terminal event types", () => {
    const qc = new QueryClient();
    const before = buildSession();
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), before);

    patchPlannerTerminalCaches(qc, buildOp(), buildEvent("text_delta", { text: "still streaming" }));

    expect(qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"))).toBe(before);
  });

  it("fans out the terminal patch across every list-prefix slice", () => {
    const qc = new QueryClient();
    qc.setQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"), buildSession());
    qc.setQueryData<PlanningSessionListResponse>(plannerKeys.list(50), {
      items: [
        {
          planning_session_id: "ps_1",
          title: "Planner kickoff",
          title_state: "custom",
          lifecycle_state: "active",
          conversation_state: "processing",
          current_question_message_id: "msg_question",
          current_operation_id: "op_running",
          current_draft_id: undefined,
          context_snapshot_id: "snap_1",
          created_at: "2026-04-21T10:00:00Z",
          updated_at: "2026-04-21T10:05:00Z",
        },
      ],
    });

    patchPlannerTerminalCaches(qc, buildOp(), buildEvent("done", {}));

    const item = qc.getQueryData<PlanningSessionListResponse>(plannerKeys.list(50))?.items[0];
    expect(item?.conversation_state).toBe("ready");
    expect(item?.current_operation_id).toBeUndefined();
  });

	it("reconciles terminal generate_title snapshots on remount and refetches canonical session state", () => {
		const qc = new QueryClient();
		const invalidateSpy = vi.spyOn(qc, "invalidateQueries");
		qc.setQueryData<PlanningSessionDetail>(
			plannerKeys.detail("ps_1"),
			buildSession({
				title: "plan_1",
				title_state: "placeholder",
				current_operation_id: "op_interactive",
				current_title_operation_id: "op_running",
			}),
		);

		reconcileTerminalOperationSnapshot(qc, buildOp({
			type: "generate_title",
			status: "succeeded",
			observed_output_text: "Generated title from durable operation",
		}));

		const detail = qc.getQueryData<PlanningSessionDetail>(plannerKeys.detail("ps_1"));
		expect(detail?.title).toBe("Generated title from durable operation");
		expect(detail?.title_state).toBe("generated");
		expect(detail?.current_title_operation_id).toBeUndefined();
		expect(detail?.current_operation_id).toBe("op_interactive");
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: plannerKeys.detail("ps_1"), exact: true });
		expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: plannerKeys.listPrefix() });
	});
});
