import { describe, expect, it } from "vitest";
import type { PlanningOperationDetail, PlanningOperationEvent } from "@/api/hooks";
import {
  buildPlannerOperationState,
  isPlannerOperationTerminal,
  reducePlannerOperationEvent,
  type PlannerOperationState,
} from "./planner-operation-controller";

function buildPolledOp(overrides: Partial<PlanningOperationDetail> = {}): PlanningOperationDetail {
  return {
    operation_id: "op_1",
    planning_session_id: "ps_1",
    type: "reply",
    status: "running",
    request: {
      content_markdown: "Tell me more",
      turn_index: 2,
      user_message_id: "msg_user",
      user_created_at: "2026-04-26T12:00:00Z",
      assistant_message_id: "msg_assistant",
      assistant_created_at: "2026-04-26T12:00:01Z",
    },
    error: null,
    result_ref: null,
    observed_output_text: null,
    created_at: "2026-04-26T12:00:00Z",
    finished_at: null,
    ...overrides,
  } as PlanningOperationDetail;
}

function buildBaseState(overrides: Partial<PlannerOperationState> = {}): PlannerOperationState {
  return {
    kind: "reply",
    operationId: "op_1",
    status: "streaming",
    lastEventSeq: 4,
    turnIndex: 2,
    userMessageId: "msg_user",
    userCreatedAt: "2026-04-26T12:00:00Z",
    assistantMessageId: "msg_assistant",
    assistantCreatedAt: "2026-04-26T12:00:01Z",
    pendingReply: { contentMarkdown: "Tell me more" },
    replyText: "partial reply",
    formalizeText: "",
    error: null,
    ...overrides,
  };
}

function buildEvent(overrides: Partial<PlanningOperationEvent> = {}): PlanningOperationEvent {
  return {
    event_id: "op_1:5",
    operation_id: "op_1",
    seq: 5,
    event_type: "cancelled",
    payload: { reason: "operation cancelled during shutdown" },
    created_at: "2026-04-26T12:00:05Z",
    ...overrides,
  } as PlanningOperationEvent;
}

describe("isPlannerOperationTerminal", () => {
  it("returns true for every terminal status", () => {
    expect(isPlannerOperationTerminal("done")).toBe(true);
    expect(isPlannerOperationTerminal("error")).toBe(true);
    expect(isPlannerOperationTerminal("cancelled")).toBe(true);
  });

  it("returns false for in-flight statuses", () => {
    expect(isPlannerOperationTerminal("pending")).toBe(false);
    expect(isPlannerOperationTerminal("streaming")).toBe(false);
  });
});

describe("PlannerOperationController — cancelled handling", () => {
  it("buildPlannerOperationState maps backend cancelled status to local cancelled (not error)", () => {
    const state = buildPlannerOperationState(
      buildPolledOp({ status: "cancelled", error: "operation cancelled during shutdown" }),
    );
    // Distinct local status — the banner uses this to render neutrally.
    // Mapping cancelled → "error" was the duct-tape-shaped option; we
    // chose the clean version so failure-rate metrics derived from
    // PlannerOperationState aren't polluted by graceful shutdowns.
    expect(state.status).toBe("cancelled");
    expect(state.error).toBe("operation cancelled during shutdown");
  });

  it("buildPlannerOperationState still maps failed → error (regression guard)", () => {
    const state = buildPlannerOperationState(
      buildPolledOp({ status: "failed", error: "boom" }),
    );
    expect(state.status).toBe("error");
    expect(state.error).toBe("boom");
  });

  it("buildPlannerOperationState hydrates observed output text for failed replies", () => {
    const state = buildPlannerOperationState(
      buildPolledOp({
        status: "failed",
        error: "provider_incomplete_turn",
        observed_output_text: "Provisional planner reply",
      }),
    );
    expect(state.status).toBe("error");
    expect(state.replyText).toBe("Provisional planner reply");
  });

  it("buildPlannerOperationState still maps succeeded → done (regression guard)", () => {
    const state = buildPlannerOperationState(buildPolledOp({ status: "succeeded" }));
    expect(state.status).toBe("done");
  });

  it("reducePlannerOperationEvent transitions to cancelled on a cancelled event", () => {
    const next = reducePlannerOperationEvent(buildBaseState(), buildEvent());
    expect(next.status).toBe("cancelled");
    expect(next.error).toBe("operation cancelled during shutdown");
    // Pending reply is cleared so the composer doesn't mistakenly
    // re-send the same payload after recovery.
    expect(next.pendingReply).toBeNull();
  });

  it("reducePlannerOperationEvent falls back to neutral copy when reason payload is missing", () => {
    const next = reducePlannerOperationEvent(buildBaseState(), buildEvent({ payload: {} }));
    expect(next.status).toBe("cancelled");
    expect(next.error).toBe("Operation cancelled");
  });

  it("reducePlannerOperationEvent on a cancelled event preserves accumulated streaming text", () => {
    // The reply text the user already saw shouldn't disappear just
    // because the operation got cancelled — the composer re-enables
    // and the assistant's mid-stream output stays visible until the
    // session refetches canonical truth.
    const next = reducePlannerOperationEvent(buildBaseState({ replyText: "the partial answer was…" }), buildEvent());
    expect(next.replyText).toBe("the partial answer was…");
  });

  it("reducePlannerOperationEvent error case still produces error status (regression guard)", () => {
    const next = reducePlannerOperationEvent(
      buildBaseState(),
      buildEvent({ event_type: "error", payload: { error: "boom" }, seq: 6 }),
    );
    expect(next.status).toBe("error");
    expect(next.error).toBe("boom");
  });
});
