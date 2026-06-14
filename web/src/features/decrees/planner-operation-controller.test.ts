import { describe, expect, it } from "vitest";
import {
  buildPlannerOperationState,
  reducePlannerOperationEvent,
} from "./planner-operation-controller";
import type { AcpRun, AcpRunEvent } from "@/api/hooks";

function buildRun(overrides: Partial<AcpRun> = {}): AcpRun {
  return {
    run_id: "96b0d561-cbe7-5c50-bf54-cd1dc3c76d47",
    session_id: "8cfa85f2-f95a-53ec-ae58-2c46f21495dc",
    planning_session_id: "ps_1",
    agent_name: "planner-reply",
    status: "created",
    created_at: "2026-04-21T10:00:00Z",
    ...overrides,
  };
}

function buildEvent(overrides: Partial<AcpRunEvent> = {}): AcpRunEvent {
  return {
    event_id: "run:1",
    run_id: "96b0d561-cbe7-5c50-bf54-cd1dc3c76d47",
    seq: 1,
    type: "run.created",
    payload: {
      assistant_message_id: "msg_assistant_2",
      assistant_created_at: "2026-04-21T10:00:01Z",
      content_markdown: "Need planner help",
      answer_payload: { text: "Need planner help" },
    },
    created_at: "2026-04-21T10:00:00Z",
    ...overrides,
  };
}

describe("planner operation controller", () => {
  it("hydrates reply lineage from ACP run.created payload", () => {
    const initial = buildPlannerOperationState(buildRun());
    const reduced = reducePlannerOperationEvent(initial, buildEvent({ payload: { ...buildEvent().payload, parent_message_id: "msg_parent_1" } }));

    expect(reduced.parentMessageId).toBe("msg_parent_1");
    expect(reduced.assistantMessageId).toBe("msg_assistant_2");
    expect(reduced.assistantCreatedAt).toBe("2026-04-21T10:00:01Z");
    expect(reduced.pendingReply?.contentMarkdown).toBe("Need planner help");
  });

  it("accumulates streamed response text on message.part", () => {
    const initial = buildPlannerOperationState(buildRun({ status: "in-progress" }));
    const reduced = reducePlannerOperationEvent(initial, buildEvent({ type: "message.part", payload: { text: "hello", planning_message_id: "msg_assistant_2", } }));

    expect(reduced.replyText).toBe("hello");
    expect(reduced.assistantMessageId).toBe("msg_assistant_2");
  });

  it("captures live reasoning from canonical kingdom.reasoning activity events", () => {
    const initial = buildPlannerOperationState(buildRun({ status: "in-progress" }));
    const withReasoning = reducePlannerOperationEvent(
      initial,
      buildEvent({
        event_id: "run:5",
        seq: 5,
        type: "generic",
        payload: {
          kind: "kingdom.reasoning",
          activity_id: "prt_reason_1",
          label: "Thinking",
          detail: "I am comparing the current transcript branch against the regenerated branch.",
        },
      }),
    );

    const thought = withReasoning.activityItems.find((item) => item.id === "prt_reason_1");
    expect(thought?.kind).toBe("thinking");
    expect(thought?.detail).toContain("current transcript branch");
  });

  it("hydrates failed reply output from terminal ACP run output_text", () => {
    const state = buildPlannerOperationState(buildRun({ status: "failed", output_text: "Partial answer", error: "planner failed" }));

    expect(state.status).toBe("error");
    expect(state.replyText).toBe("Partial answer");
    expect(state.formalizeText).toBe("");
  });

  it("hydrates failed formalize output from terminal ACP run output_text", () => {
    const state = buildPlannerOperationState(buildRun({ agent_name: "planner-formalize", status: "cancelled", output_text: "Partial decree", error: "cancelled" }));

    expect(state.status).toBe("cancelled");
    expect(state.replyText).toBe("");
    expect(state.formalizeText).toBe("Partial decree");
  });
});
