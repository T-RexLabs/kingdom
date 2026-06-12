import { describe, expect, it } from "vitest";
import { buildPlannerTranscriptViewModel } from "./planner-transcript-view-model";

describe("planner transcript view model", () => {
  it("marks only the canonical current question interactive", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          variant_index: 0,
          active: true,
          question_payload: {
            prompt: "Choose planning depth",
            type: "single_select",
            options: [{ label: "Lean", value: "lean" }],
          },
        },
        {
          planning_message_id: "user-1",
          planning_session_id: "session-1",
          turn_index: 2,
          role: "user",
          content_markdown: "Lean",
          created_at: "2026-04-21T10:01:00Z",
          variant_index: 0,
          active: true,
          answer_payload: { value: "lean", text: "Lean" },
        },
        {
          planning_message_id: "assistant-2",
          planning_session_id: "session-1",
          turn_index: 3,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:02:00Z",
          variant_index: 0,
          active: true,
          question_payload: {
            prompt: "Choose output format",
            type: "single_select",
            options: [{ label: "Spec", value: "spec" }],
          },
        },
      ],
      currentQuestionMessageId: "assistant-2",
      conversationState: "ready",
      operation: null,
    });

    expect(result.activeQuestionId).toBe("assistant-2");
    expect(result.items[0]?.isInteractive).toBe(false);
    expect(result.items[2]?.isInteractive).toBe(true);
  });

  it("resolves persisted answers from the following user message", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          variant_index: 0,
          active: true,
          question_payload: {
            prompt: "Choose planning depth",
            type: "single_select",
            options: [{ label: "Lean", value: "lean" }],
          },
        },
        {
          planning_message_id: "user-1",
          planning_session_id: "session-1",
          turn_index: 2,
          role: "user",
          content_markdown: "Lean",
          created_at: "2026-04-21T10:01:00Z",
          variant_index: 0,
          active: true,
          answer_payload: { value: "lean", text: "Lean" },
        },
      ],
      currentQuestionMessageId: undefined,
      conversationState: "ready",
      operation: null,
    });

    expect(result.items[0]?.resolvedAnswer?.value).toBe("lean");
  });

  it("locks in the active question answer from an optimistic reply before canonical user_message arrives", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          variant_index: 0,
          active: true,
          question_payload: {
            prompt: "Choose planning depth",
            type: "single_select",
            options: [{ label: "Lean", value: "lean" }],
          },
        },
      ],
      currentQuestionMessageId: "assistant-1",
      conversationState: "processing",
      operation: {
        kind: "reply",
        operationId: "operation-1",
        status: "pending",
        lastEventSeq: 0,
        turnIndex: 2,
        userMessageId: "optimistic-user-1",
        userCreatedAt: "2026-04-21T10:00:01Z",
        assistantMessageId: "optimistic-assistant-1",
        assistantCreatedAt: "2026-04-21T10:00:02Z",
        pendingReply: {
          contentMarkdown: "Lean",
          answer: { value: "lean", text: "Lean" },
        },
        replyText: "",
        formalizeText: "",
        error: null,
        ephemeralReasoningByActivityId: {},
      },
    });

    expect(result.items[0]?.resolvedAnswer?.value).toBe("lean");
  });

  it("adds optimistic user and assistant rows from an active reply operation", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          variant_index: 0,
          active: true,
          question_payload: {
            prompt: "How would you like to start?",
            type: "single_select",
            options: [
              { label: "Explore the topic", value: "explore" },
              { label: "Review current context", value: "review_context" },
              { label: "Shape a plan", value: "shape_plan" },
            ],
          },
        },
      ],
      currentQuestionMessageId: "assistant-1",
      conversationState: "processing",
      operation: {
        kind: "reply",
        operationId: "operation-1",
        status: "pending",
        lastEventSeq: 0,
        turnIndex: 2,
        userMessageId: "optimistic-user-1",
        userCreatedAt: "2026-04-21T10:00:01Z",
        assistantMessageId: "optimistic-assistant-1",
        assistantCreatedAt: "2026-04-21T10:00:01Z",
        pendingReply: {
          contentMarkdown: "Ship the planner redesign",
          answer: { text: "Ship the planner redesign" },
        },
        replyText: "",
        formalizeText: "",
        error: null,
        ephemeralReasoningByActivityId: {},
      },
    });

    expect(result.items.map((item) => item.message.planning_message_id)).toEqual([
      "assistant-1",
      "optimistic-user-1",
      "optimistic-assistant-1",
    ]);
    expect(result.items[1]?.message.content_markdown).toBe("Ship the planner redesign");
    expect(result.items[2]?.isStreaming).toBe(true);
  });

  it("does not duplicate user message when canonical cache already contains it", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          variant_index: 0,
          active: true,
          question_payload: {
            prompt: "How would you like to start?",
            type: "single_select",
            options: [
              { label: "Explore the topic", value: "explore" },
              { label: "Review current context", value: "review_context" },
              { label: "Shape a plan", value: "shape_plan" },
            ],
          },
        },
        {
          planning_message_id: "real-user-1",
          planning_session_id: "session-1",
          turn_index: 2,
          role: "user",
          content_markdown: "Ship the planner redesign",
          created_at: "2026-04-21T10:00:01Z",
          variant_index: 0,
          active: true,
          answer_payload: { text: "Ship the planner redesign" },
        },
      ],
      currentQuestionMessageId: "assistant-1",
      conversationState: "processing",
      operation: {
        kind: "reply",
        operationId: "operation-1",
        status: "streaming",
        lastEventSeq: 2,
        turnIndex: 2,
        userMessageId: "real-user-1",
        userCreatedAt: "2026-04-21T10:00:01Z",
        assistantMessageId: "real-assistant-2",
        assistantCreatedAt: "2026-04-21T10:00:02Z",
        pendingReply: { contentMarkdown: "Ship the planner redesign" },
        replyText: "Streaming response...",
        formalizeText: "",
        error: null,
        ephemeralReasoningByActivityId: {},
      },
    });

    const ids = result.items.map((item) => item.message.planning_message_id);
    expect(ids.filter((id) => id === "real-user-1")).toHaveLength(1);
    expect(ids).toContain("real-assistant-2");
    expect(result.items.find((item) => item.message.planning_message_id === "real-assistant-2")?.isStreaming).toBe(true);
  });

  it("marks streamed question as active but not interactive until operation completes", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          variant_index: 0,
          active: true,
          question_payload: {
            prompt: "Opening question",
            type: "long_text",
          },
        },
      ],
      currentQuestionMessageId: "assistant-1",
      conversationState: "processing",
      operation: {
        kind: "reply",
        operationId: "operation-1",
        status: "streaming",
        lastEventSeq: 2,
        turnIndex: 2,
        userMessageId: "user-2",
        userCreatedAt: "2026-04-21T10:00:01Z",
        assistantMessageId: "assistant-2",
        assistantCreatedAt: "2026-04-21T10:00:02Z",
        pendingReply: { contentMarkdown: "Need a cleaner UX" },
        replyText: 'Context text.\n```structured_question\n{"prompt":"Choose planning depth","type":"single_select","options":[{"label":"Lean","value":"lean"}]}\n```',
        formalizeText: "",
        error: null,
        ephemeralReasoningByActivityId: {},
      },
    });

    expect(result.activeQuestionId).toBe("assistant-2");
    expect(result.activeQuestion?.prompt).toBe("Choose planning depth");
    // Streaming messages are not interactive — the user must wait for the operation to complete
    expect(result.items[result.items.length - 1]?.isInteractive).toBe(false);
  });

  it("retains failed reply context as non-streaming provisional output", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          variant_index: 0,
          active: true,
          question_payload: { prompt: "Pick one", type: "long_text" },
        },
      ],
      currentQuestionMessageId: "assistant-1",
      conversationState: "ready",
      operation: {
        kind: "reply",
        operationId: "operation-failed",
        status: "error",
        lastEventSeq: 5,
        turnIndex: 2,
        userMessageId: "optimistic-user-1",
        userCreatedAt: "2026-04-21T10:00:01Z",
        assistantMessageId: "optimistic-assistant-1",
        assistantCreatedAt: "2026-04-21T10:00:01Z",
        pendingReply: { contentMarkdown: "abandoned" },
        replyText: "partial stream that never finished",
        formalizeText: "",
        error: "missing completion receipt evidence id",
        ephemeralReasoningByActivityId: {},
      },
    });

    expect(result.items.map((item) => item.message.planning_message_id)).toEqual([
      "assistant-1",
      "optimistic-user-1",
      "optimistic-assistant-1",
    ]);
    expect(result.items[2]?.isStreaming).toBe(false);
    expect(result.activeQuestionId).toBe("assistant-1");
  });

  it("retains cancelled regenerate output as non-streaming provisional context", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          turn_index: 1,
          role: "assistant",
          content_markdown: "stable",
          created_at: "2026-04-21T10:00:00Z",
          variant_index: 0,
          active: true,
        },
      ],
      currentQuestionMessageId: undefined,
      conversationState: "ready",
      operation: {
        kind: "regenerate",
        operationId: "operation-cancelled",
        status: "cancelled",
        lastEventSeq: 5,
        turnIndex: 1,
        targetTurnIndex: 1,
        userMessageId: undefined,
        userCreatedAt: undefined,
        assistantMessageId: "optimistic-regen-1",
        assistantCreatedAt: "2026-04-21T10:00:01Z",
        pendingReply: null,
        replyText: "half a regeneration",
        formalizeText: "",
        error: "operation cancelled during shutdown",
        ephemeralReasoningByActivityId: {},
      },
    });

    expect(result.items.map((item) => item.message.planning_message_id)).toEqual(["assistant-1", "optimistic-regen-1"]);
    expect(result.items[1]?.isStreaming).toBe(false);
  });
});
