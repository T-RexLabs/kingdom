import type { AcpRun, AcpRunEvent, PlanningMessage } from "@/api/hooks";

export type QuestionPayload = NonNullable<PlanningMessage["question_payload"]>;
export type AnswerPayload = NonNullable<PlanningMessage["answer_payload"]>;

export type PlannerPendingReply = {
  contentMarkdown: string;
  answer?: AnswerPayload;
};

export type PlannerActivityKind = "thinking" | "tool" | "progress";

export type PlannerActivityItem = {
  id: string;
  kind: PlannerActivityKind;
  label: string;
  detail?: string;
  input?: string;
  output?: string;
  occurredAt?: string;
};

export type PlannerOperationKind = "reply" | "formalize" | "regenerate" | "generate_title";

/**
 * One source of truth for which planner operation statuses are
 * terminal. Every consumer that decides "is this operation done with"
 * — clear-active-operation effects, generating-indicator gates, ghost
 * streaming entries, global-formalize notification cleanup — should
 * read from here instead of hard-coding the alphabet. A future status
 * (e.g. `paused`) becomes a one-line edit, not a search across the
 * codebase that's likely to miss a consumer.
 */
export function isPlannerOperationTerminal(
  status: PlannerOperationState["status"],
): boolean {
  return status === "done" || status === "error" || status === "cancelled";
}

export type PlannerOperationState = {
  kind: PlannerOperationKind;
  operationId: string;
  /**
   * `cancelled` is distinct from `error` so the UI can render shutdown
   * cancellations neutrally instead of as failures, and so failure-rate
   * metrics derived from this state aren't polluted by graceful
   * shutdowns. The cancellation reason lives in `error` (the column
   * name on the row) — only the *status* differs.
   */
  status: "pending" | "streaming" | "done" | "error" | "cancelled";
  lastEventSeq: number;
  // Parent the operation's new branch attaches to: the user message's parent
  // for reply/edit, or the regenerated assistant's parent for regenerate.
  parentMessageId?: string;
  userMessageId?: string;
  userCreatedAt?: string;
  assistantMessageId?: string;
  assistantCreatedAt?: string;
  pendingReply: PlannerPendingReply | null;
  replyText: string;
  formalizeText: string;
  draftId?: string;
  error: string | null;
  events?: AcpRunEvent[];
  activityItems: PlannerActivityItem[];
};

export function buildPlannerOperationState(operation: AcpRun): PlannerOperationState {
  const kind: PlannerOperationKind =
    operation.agent_name === "planner-generate-title" ? "generate_title" :
    operation.agent_name === "planner-formalize" ? "formalize" :
    operation.agent_name === "planner-regenerate" ? "regenerate" : "reply";
  const outputText = typeof operation.output_text === "string" ? operation.output_text : "";
  return {
    kind,
    operationId: operation.run_id,
    status:
      operation.status === "failed" ? "error" :
      operation.status === "cancelled" ? "cancelled" :
      operation.status === "completed" ? "done" :
      operation.status === "in-progress" ? "streaming" : "pending",
    lastEventSeq: 0,
    pendingReply: null,
    replyText: kind === "formalize" || kind === "generate_title" ? "" : outputText,
    formalizeText: kind === "formalize" ? outputText : "",
    error: operation.error ?? null,
    events: [],
    activityItems: [],
  };
}

export function reducePlannerOperationEvent(
  state: PlannerOperationState,
  event: AcpRunEvent
): PlannerOperationState {
  if (event.seq <= state.lastEventSeq) {
    return state;
  }
  const next = {
    ...state,
    status: state.status === "pending" ? "streaming" : state.status,
    lastEventSeq: event.seq,
    events: [...(state.events ?? []), event],
  } satisfies PlannerOperationState;

  switch (event.type) {
    case "run.created":
      return applyOperationPayload({ ...next, status: "pending" }, event.payload);
    case "run.in-progress":
      return applyOperationPayload(next, event.payload);
    case "generic": {
      const activity = parsePlannerActivityEvent(event);
      if (!activity) {
        return next;
      }
      return {
        ...next,
        activityItems: upsertPlannerActivity(next.activityItems, activity),
      };
    }
    case "message.part": {
      const withPayload = applyOperationPayload(next, event.payload);
      const text = readString(event.payload.text);
      if (!text) {
        return withPayload;
      }
      if (withPayload.kind === "formalize") {
        return {
          ...withPayload,
          formalizeText: `${withPayload.formalizeText}${text}`,
        };
      }
      return {
        ...withPayload,
        replyText: `${withPayload.replyText}${text}`,
      };
    }
    case "message.completed": {
      const withPayload = applyOperationPayload(next, event.payload);
      const text = readString(event.payload.text);
      if (!text) {
        return withPayload;
      }
      if (withPayload.kind === "formalize") {
          return { ...withPayload, formalizeText: text };
      }
      return { ...withPayload, replyText: text };
    }
    case "run.completed": {
      if (next.kind === "formalize") {
        return {
          ...next,
          status: "done",
          draftId: readString(event.payload.planning_draft_id) ?? next.draftId,
        };
      }
      return {
        ...next,
        status: "done",
        pendingReply: null,
      };
    }
    case "run.failed":
    case "error": {
      return {
        ...next,
        status: "error",
        error: readString(event.payload.error) ?? "Planner operation failed",
      };
    }
    case "run.cancelled": {
      return {
        ...next,
        status: "cancelled",
        error: readString(event.payload.reason) ?? "Operation cancelled",
        pendingReply: null,
      };
    }
    default:
      return next;
  }
}

function parsePlannerActivityEvent(event: AcpRunEvent): PlannerActivityItem | null {
  const kind = readString(event.payload.kind);
  const id = readString(event.payload.activity_id) ?? event.event_id;
  if (!kind || !id) {
    return null;
  }
  switch (kind) {
    case "kingdom.phase": {
      const label = readString(event.payload.label);
      if (!label) {
        return null;
      }
      return { id, kind: "progress", label, occurredAt: event.created_at };
    }
    case "kingdom.progress": {
      const label = readString(event.payload.label);
      if (!label) {
        return null;
      }
      return { id, kind: "progress", label, occurredAt: event.created_at };
    }
    case "kingdom.tool": {
      const label = readString(event.payload.label);
      if (!label) {
        return null;
      }
      return {
        id,
        kind: "tool",
        label,
        detail: readString(event.payload.detail),
        input: readString(event.payload.tool_input),
        occurredAt: event.created_at,
      };
    }
    case "kingdom.reasoning": {
      return {
        id,
        kind: "thinking",
        label: readString(event.payload.label) ?? "Thinking",
        detail: readString(event.payload.detail) ?? readString(event.payload.summary),
        occurredAt: event.created_at,
      };
    }
    default:
      return null;
  }
}

function upsertPlannerActivity(items: PlannerActivityItem[], nextItem: PlannerActivityItem): PlannerActivityItem[] {
  const index = items.findIndex((item) => item.id === nextItem.id);
  if (index < 0) {
    return [...items, nextItem];
  }
  const current = items[index];
  const merged = {
    ...current,
    ...nextItem,
    detail: nextItem.detail ?? current.detail,
    input: nextItem.input ?? current.input,
    output: nextItem.output ?? current.output,
    occurredAt: nextItem.occurredAt ?? current.occurredAt,
  } satisfies PlannerActivityItem;
  const copy = items.slice();
  copy[index] = merged;
  return copy;
}

function applyOperationPayload(
  state: PlannerOperationState,
  payload: AcpRunEvent["payload"],
): PlannerOperationState {
  const next = { ...state };
  const parentMessageId = readString(payload.parent_message_id);
  if (parentMessageId) {
    next.parentMessageId = parentMessageId;
  }
  const userMessageId = readString(payload.user_message_id);
  if (userMessageId) {
    next.userMessageId = userMessageId;
  }
  const userCreatedAt = readString(payload.user_created_at);
  if (userCreatedAt) {
    next.userCreatedAt = userCreatedAt;
  }
  const assistantMessageId = readString(payload.assistant_message_id) ?? readString(payload.planning_message_id);
  if (assistantMessageId) {
    next.assistantMessageId = assistantMessageId;
  }
  const assistantCreatedAt = readString(payload.assistant_created_at) ?? readString(payload.created_at);
  if (assistantCreatedAt) {
    next.assistantCreatedAt = assistantCreatedAt;
  }
  const draftId = readString(payload.planning_draft_id);
  if (draftId) {
    next.draftId = draftId;
  }
  if (next.kind === "reply") {
    const contentMarkdown = readString(payload.content_markdown);
    const answer = parseAnswerPayload(payload.answer_payload);
    if (contentMarkdown || answer) {
      next.pendingReply = {
        contentMarkdown: contentMarkdown ?? next.pendingReply?.contentMarkdown ?? "",
        ...(answer ? { answer } : next.pendingReply?.answer ? { answer: next.pendingReply.answer } : {}),
      };
    }
  }
  return next;
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function parseAnswerPayload(value: unknown): AnswerPayload | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  return value as AnswerPayload;
}
