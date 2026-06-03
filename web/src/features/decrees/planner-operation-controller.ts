import type { PlanningMessage, PlanningOperationDetail, PlanningOperationEvent } from "@/api/hooks";

export type QuestionPayload = NonNullable<PlanningMessage["question_payload"]>;
export type AnswerPayload = NonNullable<PlanningMessage["answer_payload"]>;

export type PlannerPendingReply = {
  contentMarkdown: string;
  answer?: AnswerPayload;
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
  turnIndex?: number;
  targetTurnIndex?: number;
  userMessageId?: string;
  userCreatedAt?: string;
  assistantMessageId?: string;
  assistantCreatedAt?: string;
  pendingReply: PlannerPendingReply | null;
  replyText: string;
  formalizeText: string;
  draftId?: string;
  error: string | null;
};

export function buildPlannerOperationState(operation: PlanningOperationDetail): PlannerOperationState {
  const kind: PlannerOperationKind =
    operation.type === "generate_title" ? "generate_title" :
    operation.type === "formalize" ? "formalize" :
    operation.type === "regenerate" ? "regenerate" : "reply";
  const observedOutputText = typeof operation.observed_output_text === "string" ? operation.observed_output_text : "";
  return {
    kind,
    operationId: operation.operation_id,
    // Map the persisted operation status to the local PlannerOperationState
    // status. `cancelled` is preserved as its own state so the UI can
    // render it neutrally (server restart, not a planner failure).
    status:
      operation.status === "failed" ? "error" :
      operation.status === "cancelled" ? "cancelled" :
      operation.status === "succeeded" ? "done" : "pending",
    lastEventSeq: 0,
    turnIndex: readNumber(operation.request.turn_index),
    targetTurnIndex:
      operation.type === "regenerate"
        ? readNumber(operation.request.turn_index)
        : undefined,
    userMessageId: readString(operation.request.user_message_id),
    userCreatedAt: readDateString(operation.request.user_created_at),
    assistantMessageId: readString(operation.request.assistant_message_id),
    assistantCreatedAt: readDateString(operation.request.assistant_created_at),
    pendingReply:
      operation.type === "reply"
        ? {
            contentMarkdown: String(operation.request.content_markdown ?? ""),
            answer: parseAnswerPayload(operation.request.answer_payload),
          }
        : null,
    replyText: kind === "reply" || kind === "regenerate" ? observedOutputText : "",
    formalizeText: kind === "formalize" ? observedOutputText : "",
    draftId: typeof operation.result_ref === "string" ? operation.result_ref : undefined,
    error: operation.error ?? null,
  };
}

export function reducePlannerOperationEvent(
  state: PlannerOperationState,
  event: PlanningOperationEvent
): PlannerOperationState {
  const next = {
    ...state,
    status: state.status === "pending" ? "streaming" : state.status,
    lastEventSeq: Math.max(state.lastEventSeq, event.seq),
  } satisfies PlannerOperationState;

  switch (event.event_type) {
    case "user_message":
      return next;
    case "text_delta": {
      const text = readString(event.payload.text);
      if (!text) {
        return next;
      }
      if (next.kind === "formalize") {
        return {
          ...next,
          formalizeText: `${next.formalizeText}${text}`,
        };
      }
      return {
        ...next,
        replyText: `${next.replyText}${text}`,
      };
    }
    case "done": {
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
    case "error": {
      return {
        ...next,
        status: "error",
        error: readString(event.payload.error) ?? "Planner operation failed",
      };
    }
    case "cancelled": {
      // Distinct from `error` so the banner can render neutrally and
      // metrics derived from this status don't conflate shutdown with
      // genuine failure. The reason carries through `error` (the
      // payload field name reflects the column on the operation row).
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

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function readNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function readDateString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function parseAnswerPayload(value: unknown): AnswerPayload | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  return value as AnswerPayload;
}
