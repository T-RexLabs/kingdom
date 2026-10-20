import { describe, expect, it } from "vitest";
import { buildPlannerTranscriptViewModel } from "./planner-transcript-view-model";
import type { PlannerOperationState } from "./planner-operation-controller";

function buildOperation(overrides: Partial<PlannerOperationState>): PlannerOperationState {
  return {
    kind: "reply",
    operationId: "operation-1",
    status: "pending",
    lastEventSeq: 0,
    pendingReply: null,
    replyText: "",
    formalizeText: "",
    error: null,
    events: [],
    activityItems: [],
    ...overrides,
  };
}

describe("planner transcript view model", () => {
  it("renders the active path directly and appends a streaming ghost for an active reply", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        { planning_message_id: "opening", planning_session_id: "s", role: "assistant", content_markdown: "Hi", created_at: "t" },
        { planning_message_id: "user-1", planning_session_id: "s", parent_message_id: "opening", role: "user", content_markdown: "Q", created_at: "t" },
        { planning_message_id: "assistant-1", planning_session_id: "s", parent_message_id: "user-1", role: "assistant", content_markdown: "A", created_at: "t" },
      ],
      conversationState: "processing",
      operation: buildOperation({
        kind: "reply",
        status: "streaming",
        operationId: "op-reply",
        parentMessageId: "assistant-1",
        userMessageId: "pending-user",
        userCreatedAt: "t",
        assistantMessageId: "pending-assistant",
        assistantCreatedAt: "t",
        pendingReply: { contentMarkdown: "Next question" },
        replyText: "Streaming answer",
      }),
    });

    const ids = result.items.map((item) => item.message.planning_message_id);
    // The active path renders verbatim, with the optimistic user + streaming
    // assistant ghosts appended.
    expect(ids).toEqual(["opening", "user-1", "assistant-1", "pending-user", "pending-assistant"]);
    expect(result.items.find((i) => i.message.planning_message_id === "pending-assistant")?.isStreaming).toBe(true);
  });

  it("marks only the canonical current question interactive", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          question_payload: {
            prompt: "Choose planning depth",
            type: "single_select",
            options: [{ label: "Lean", value: "lean" }],
          },
        },
        {
          planning_message_id: "user-1",
          planning_session_id: "session-1",
          role: "user",
          content_markdown: "Lean",
          created_at: "2026-04-21T10:01:00Z",
          answer_payload: { value: "lean", text: "Lean" },
        },
        {
          planning_message_id: "assistant-2",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:02:00Z",
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
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          question_payload: {
            prompt: "Choose planning depth",
            type: "single_select",
            options: [{ label: "Lean", value: "lean" }],
          },
        },
        {
          planning_message_id: "user-1",
          planning_session_id: "session-1",
          role: "user",
          content_markdown: "Lean",
          created_at: "2026-04-21T10:01:00Z",
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
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          question_payload: {
            prompt: "Choose planning depth",
            type: "single_select",
            options: [{ label: "Lean", value: "lean" }],
          },
        },
      ],
      currentQuestionMessageId: "assistant-1",
      conversationState: "processing",
      operation: buildOperation({
        userMessageId: "optimistic-user-1",
        userCreatedAt: "2026-04-21T10:00:01Z",
        assistantMessageId: "optimistic-assistant-1",
        assistantCreatedAt: "2026-04-21T10:00:02Z",
        pendingReply: {
          contentMarkdown: "Lean",
          answer: { value: "lean", text: "Lean" },
        },
      }),
    });

    expect(result.items[0]?.resolvedAnswer?.value).toBe("lean");
  });

  it("adds optimistic user and assistant rows from an active reply operation", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
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
      operation: buildOperation({
        userMessageId: "optimistic-user-1",
        userCreatedAt: "2026-04-21T10:00:01Z",
        assistantMessageId: "optimistic-assistant-1",
        assistantCreatedAt: "2026-04-21T10:00:01Z",
        pendingReply: {
          contentMarkdown: "Ship the planner redesign",
          answer: { text: "Ship the planner redesign" },
        },
      }),
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
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
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
          role: "user",
          content_markdown: "Ship the planner redesign",
          created_at: "2026-04-21T10:00:01Z",
          answer_payload: { text: "Ship the planner redesign" },
        },
      ],
      currentQuestionMessageId: "assistant-1",
      conversationState: "processing",
      operation: buildOperation({
        status: "streaming",
        lastEventSeq: 2,
        userMessageId: "real-user-1",
        userCreatedAt: "2026-04-21T10:00:01Z",
        assistantMessageId: "real-assistant-2",
        assistantCreatedAt: "2026-04-21T10:00:02Z",
        pendingReply: { contentMarkdown: "Ship the planner redesign" },
        replyText: "Streaming response...",
      }),
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
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          question_payload: {
            prompt: "Opening question",
            type: "long_text",
          },
        },
      ],
      currentQuestionMessageId: "assistant-1",
      conversationState: "processing",
      operation: buildOperation({
        status: "streaming",
        lastEventSeq: 2,
        userMessageId: "user-2",
        userCreatedAt: "2026-04-21T10:00:01Z",
        assistantMessageId: "assistant-2",
        assistantCreatedAt: "2026-04-21T10:00:02Z",
        pendingReply: { contentMarkdown: "Need a cleaner UX" },
        replyText: 'Context text.\n```structured_question\n{"prompt":"Choose planning depth","type":"single_select","options":[{"label":"Lean","value":"lean"}]}\n```',
      }),
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
          role: "assistant",
          content_markdown: "",
          created_at: "2026-04-21T10:00:00Z",
          question_payload: { prompt: "Pick one", type: "long_text" },
        },
      ],
      currentQuestionMessageId: "assistant-1",
      conversationState: "ready",
      operation: buildOperation({
        operationId: "operation-failed",
        status: "error",
        lastEventSeq: 5,
        userMessageId: "optimistic-user-1",
        userCreatedAt: "2026-04-21T10:00:01Z",
        assistantMessageId: "optimistic-assistant-1",
        assistantCreatedAt: "2026-04-21T10:00:01Z",
        pendingReply: { contentMarkdown: "abandoned" },
        replyText: "partial stream that never finished",
        error: "provider operation did not settle",
      }),
    });

    expect(result.items.map((item) => item.message.planning_message_id)).toEqual([
      "assistant-1",
      "optimistic-user-1",
      "optimistic-assistant-1",
    ]);
    expect(result.items[2]?.isStreaming).toBe(false);
    expect(result.items[2]?.isCanonical).toBe(false);
    expect(result.items[0]?.isCanonical).toBe(true);
    expect(result.activeQuestionId).toBe("assistant-1");
  });

  it("retains cancelled regenerate output as non-streaming provisional context", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "stable",
          created_at: "2026-04-21T10:00:00Z",
        },
      ],
      currentQuestionMessageId: undefined,
      conversationState: "ready",
      operation: buildOperation({
        kind: "regenerate",
        operationId: "operation-cancelled",
        status: "cancelled",
        lastEventSeq: 5,
        assistantMessageId: "optimistic-regen-1",
        assistantCreatedAt: "2026-04-21T10:00:01Z",
        replyText: "half a regeneration",
        error: "operation cancelled during shutdown",
      }),
    });

    expect(result.items.map((item) => item.message.planning_message_id)).toEqual(["assistant-1", "optimistic-regen-1"]);
    expect(result.items[1]?.isStreaming).toBe(false);
    expect(result.items[1]?.isCanonical).toBe(false);
  });

  it("hides stale descendants for an optimistic edit reply", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "root",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "Root",
          created_at: "2026-04-21T10:00:00Z",
        },
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          parent_message_id: "root",
          role: "assistant",
          content_markdown: "Original answer",
          created_at: "2026-04-21T10:01:00Z",
        },
        {
          planning_message_id: "stale-user",
          planning_session_id: "session-1",
          parent_message_id: "assistant-1",
          role: "user",
          content_markdown: "Old follow-up",
          created_at: "2026-04-21T10:02:00Z",
        },
        {
          planning_message_id: "stale-assistant",
          planning_session_id: "session-1",
          parent_message_id: "stale-user",
          role: "assistant",
          content_markdown: "Old branch answer",
          created_at: "2026-04-21T10:03:00Z",
        },
      ],
      conversationState: "processing",
      operation: buildOperation({
        kind: "reply",
        status: "streaming",
        operationId: "op-edit",
        parentMessageId: "root",
        userMessageId: "pending-user",
        userCreatedAt: "2026-04-21T10:04:00Z",
        assistantMessageId: "pending-assistant",
        assistantCreatedAt: "2026-04-21T10:04:01Z",
        pendingReply: { contentMarkdown: "Edited question" },
        replyText: "Edited answer",
      }),
    });

    expect(result.items.map((item) => item.message.planning_message_id)).toEqual([
      "root",
      "pending-user",
      "pending-assistant",
    ]);
  });

  it("hides stale descendants for an optimistic regenerate", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "root",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "Root",
          created_at: "2026-04-21T10:00:00Z",
        },
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          parent_message_id: "root",
          role: "assistant",
          content_markdown: "Original answer",
          created_at: "2026-04-21T10:01:00Z",
        },
        {
          planning_message_id: "stale-user",
          planning_session_id: "session-1",
          parent_message_id: "assistant-1",
          role: "user",
          content_markdown: "Old follow-up",
          created_at: "2026-04-21T10:02:00Z",
        },
      ],
      conversationState: "processing",
      operation: buildOperation({
        kind: "regenerate",
        status: "streaming",
        operationId: "op-regen",
        parentMessageId: "root",
        assistantMessageId: "pending-regen",
        assistantCreatedAt: "2026-04-21T10:04:00Z",
        replyText: "Regenerated answer",
      }),
    });

    expect(result.items.map((item) => item.message.planning_message_id)).toEqual([
      "root",
      "pending-regen",
    ]);
  });

  it("keeps stale descendants hidden after regenerate message patch until refetch catches up", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "root",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "Root",
          created_at: "2026-04-21T10:00:00Z",
        },
        {
          planning_message_id: "regen-new",
          planning_session_id: "session-1",
          parent_message_id: "root",
          role: "assistant",
          content_markdown: "Canonical regenerated answer",
          source_run_id: "op-regen",
          created_at: "2026-04-21T10:04:00Z",
        },
        {
          planning_message_id: "stale-user",
          planning_session_id: "session-1",
          parent_message_id: "assistant-1",
          role: "user",
          content_markdown: "Old follow-up",
          created_at: "2026-04-21T10:02:00Z",
        },
      ],
      conversationState: "processing",
      operation: buildOperation({
        kind: "regenerate",
        status: "streaming",
        operationId: "op-regen",
        parentMessageId: "root",
        assistantMessageId: "regen-new",
        assistantCreatedAt: "2026-04-21T10:04:00Z",
        replyText: "Canonical regenerated answer",
      }),
    });

    expect(result.items.map((item) => item.message.planning_message_id)).toEqual([
      "root",
      "regen-new",
    ]);
  });

  it("keeps stale descendants hidden after edit reply patch until refetch catches up", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "root",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "Root",
          created_at: "2026-04-21T10:00:00Z",
        },
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          parent_message_id: "root",
          role: "assistant",
          content_markdown: "Original answer",
          created_at: "2026-04-21T10:01:00Z",
        },
        {
          planning_message_id: "stale-user",
          planning_session_id: "session-1",
          parent_message_id: "assistant-1",
          role: "user",
          content_markdown: "Old follow-up",
          created_at: "2026-04-21T10:02:00Z",
        },
        {
          planning_message_id: "reply-new",
          planning_session_id: "session-1",
          parent_message_id: "pending-user",
          role: "assistant",
          content_markdown: "Canonical edited answer",
          source_run_id: "op-edit",
          created_at: "2026-04-21T10:04:01Z",
        },
      ],
      conversationState: "processing",
      operation: buildOperation({
        kind: "reply",
        status: "streaming",
        operationId: "op-edit",
        parentMessageId: "root",
        userMessageId: "pending-user",
        userCreatedAt: "2026-04-21T10:04:00Z",
        assistantMessageId: "reply-new",
        assistantCreatedAt: "2026-04-21T10:04:01Z",
        pendingReply: { contentMarkdown: "Edited question" },
        replyText: "Canonical edited answer",
      }),
    });

    expect(result.items.map((item) => item.message.planning_message_id)).toEqual([
      "root",
      "pending-user",
      "reply-new",
    ]);
  });

  it("keeps the canonical assistant message live-bound until the active run finishes", () => {
    const result = buildPlannerTranscriptViewModel({
      messages: [
        {
          planning_message_id: "assistant-1",
          planning_session_id: "session-1",
          role: "assistant",
          content_markdown: "Final text already patched",
          source_run_id: "operation-1",
          created_at: "2026-04-21T10:00:00Z",
        },
      ],
      currentQuestionMessageId: undefined,
      conversationState: "processing",
      operation: buildOperation({
        status: "streaming",
        operationId: "operation-1",
        assistantMessageId: "assistant-1",
        activityItems: [{ id: "tool-1", eventId: "event-1", sequence: 1, kind: "tool", toolName: "webfetch", toolStatus: "completed", label: "Fetched 3 web sources" }],
      }),
    });

    expect(result.items).toHaveLength(1);
    expect(result.items[0]?.isStreaming).toBe(true);
    expect(result.items[0]?.liveActivityItems?.[0]?.label).toBe("Fetched 3 web sources");
  });
});
