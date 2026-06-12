import type { PlanningMessage } from "@/api/hooks";
import {
  isPlannerOperationTerminal,
  type AnswerPayload,
  type PlannerActivityItem,
  type PlannerOperationState,
  type QuestionPayload,
} from "./planner-operation-controller";
import { parseStreamingQuestion } from "./parse-streaming-question";

export type ParsedStreamingReply = ReturnType<typeof parseStreamingQuestion>;

export type TranscriptItem = {
  kind: "message";
  key: string;
  message: PlanningMessage;
  resolvedAnswer?: AnswerPayload;
  isInteractive: boolean;
  isStreaming?: boolean;
  contentMarkdown?: string;
  variantCount?: number;
  currentVariantIndex?: number;
  activityRunId?: string;
  liveActivityItems?: PlannerActivityItem[];
};

type BuildPlannerTranscriptInput = {
  messages: PlanningMessage[];
  currentQuestionMessageId?: string;
  conversationState: string;
  operation?: PlannerOperationState | null;
  /**
   * Optional pre-parsed streaming reply. Pass it from a `useMemo` keyed on
   * `operation.replyText` so we parse the growing text exactly once per
   * delta instead of four times (once per call site below). Falls back to
   * lazy parsing if the caller does not provide it.
   */
  parsedReply?: ParsedStreamingReply | null;
};

type TranscriptBuildResult = {
  items: TranscriptItem[];
  activeQuestion?: QuestionPayload;
  activeQuestionId?: string;
};

export function buildPlannerTranscriptViewModel({
  messages,
  currentQuestionMessageId,
  conversationState,
  operation,
  parsedReply,
}: BuildPlannerTranscriptInput): TranscriptBuildResult {
  const activeOperationRunId = operation && !operation.operationId.startsWith("pending-")
    ? operation.operationId
    : undefined;
  const resolvedAnswersByQuestionId = persistedAnswersByQuestionId(messages, currentQuestionMessageId, operation);
  // Resolve `parsedReply` once: prefer the caller's memoised value, fall back
  // to a single parse here if the caller did not supply one. The four call
  // sites below all reuse this resolution rather than re-running the parser.
  const reply: ParsedStreamingReply | null = parsedReply ?? (operation?.replyText
    ? parseStreamingQuestion(operation.replyText)
    : null);
  const activeQuestionId = resolveActiveQuestionId(currentQuestionMessageId, operation, reply);
  const activeQuestion = resolveActiveQuestion(messages, activeQuestionId, operation, reply);

  const items: TranscriptItem[] = messages.map((message) => ({
    kind: "message",
    key: `message-${message.planning_message_id}`,
    message,
    resolvedAnswer: resolvedAnswersByQuestionId[message.planning_message_id],
    isInteractive:
      conversationState === "ready" &&
      message.role === "assistant" &&
      Boolean(message.question_payload) &&
      message.planning_message_id === activeQuestionId,
    variantCount: message.variant_count,
    currentVariantIndex: message.variant_index,
    activityRunId: message.role === "assistant" ? message.source_run_id : undefined,
  }));

  if (operation?.kind === "regenerate" && operation.status !== "done" && operation.replyText.trim() !== "") {
    const targetTurn = operation.targetTurnIndex ?? operation.turnIndex;
    const canonicalIds = new Set(messages.map((m) => m.planning_message_id));
    if (targetTurn && operation.assistantMessageId && !canonicalIds.has(operation.assistantMessageId) && operation.assistantCreatedAt) {
      const parsed = reply ?? parseStreamingQuestion(operation.replyText);
      items.push({
        kind: "message",
        key: `message-${operation.assistantMessageId}`,
        message: {
          planning_message_id: operation.assistantMessageId,
          planning_session_id: messages[messages.length - 1]?.planning_session_id ?? "",
          turn_index: targetTurn,
          role: "assistant",
          content_markdown: parsed.content,
          source_run_id: activeOperationRunId,
          created_at: operation.assistantCreatedAt,
          question_payload: parsed.question ?? undefined,
          variant_index: 0,
          active: true,
        },
        resolvedAnswer: undefined,
        isInteractive: false,
        isStreaming: operation.status === "streaming" || operation.status === "pending",
        contentMarkdown: parsed.content,
        activityRunId: activeOperationRunId,
        liveActivityItems: operation.activityItems,
      });
    }
  }

  if (operation?.kind === "reply" && operation.status !== "done") {
    const canonicalIds = new Set(messages.map((m) => m.planning_message_id));

    if (operation.pendingReply && operation.userMessageId && !canonicalIds.has(operation.userMessageId) && operation.turnIndex && operation.userCreatedAt) {
      items.push({
        kind: "message",
        key: `message-${operation.userMessageId}`,
        message: {
          planning_message_id: operation.userMessageId,
          planning_session_id: messages[messages.length - 1]?.planning_session_id ?? "",
          turn_index: operation.turnIndex,
          role: "user",
          content_markdown: operation.pendingReply.contentMarkdown,
          created_at: operation.userCreatedAt,
          answer_payload: operation.pendingReply.answer,
          variant_index: 0,
          active: true,
        },
        resolvedAnswer: undefined,
        isInteractive: false,
      });
    }

    if (operation.assistantMessageId && !canonicalIds.has(operation.assistantMessageId) && operation.turnIndex && operation.assistantCreatedAt) {
      const parsed = reply ?? parseStreamingQuestion(operation.replyText);
      items.push({
        kind: "message",
        key: `message-${operation.assistantMessageId}`,
        message: {
          planning_message_id: operation.assistantMessageId,
          planning_session_id: messages[messages.length - 1]?.planning_session_id ?? "",
          turn_index: operation.turnIndex,
          role: "assistant",
          content_markdown: parsed.content,
          source_run_id: activeOperationRunId,
          created_at: operation.assistantCreatedAt,
          question_payload: parsed.question ?? undefined,
          variant_index: 0,
          active: true,
        },
        resolvedAnswer: undefined,
        isInteractive: false,
        isStreaming: operation.status === "streaming" || operation.status === "pending",
        contentMarkdown: parsed.content,
        activityRunId: activeOperationRunId,
        liveActivityItems: operation.activityItems,
      });
    }
  }

  const itemsWithLiveOperation = attachLiveOperationState(items, operation, activeOperationRunId);

  return {
    items: itemsWithLiveOperation,
    activeQuestion,
    activeQuestionId,
  };
}

function attachLiveOperationState(
  items: TranscriptItem[],
  operation: PlannerOperationState | null | undefined,
  activeOperationRunId: string | undefined,
): TranscriptItem[] {
  if (!operation || isPlannerOperationTerminal(operation.status)) {
    return items;
  }
  const assistantMessageId = operation.assistantMessageId?.trim();
  if (!assistantMessageId && !activeOperationRunId) {
    return items;
  }

  return items.map((item) => {
    if (item.message.role !== "assistant") {
      return item;
    }
    const matchesAssistantMessage = assistantMessageId != null && item.message.planning_message_id === assistantMessageId;
    const matchesRun = activeOperationRunId != null && item.message.source_run_id === activeOperationRunId;
    if (!matchesAssistantMessage && !matchesRun) {
      return item;
    }
    return {
      ...item,
      isStreaming: operation.status === "streaming" || operation.status === "pending",
      activityRunId: activeOperationRunId,
      liveActivityItems: operation.activityItems,
    };
  });
}

function resolveActiveQuestionId(
  currentQuestionMessageId: string | undefined,
  operation: PlannerOperationState | null | undefined,
  reply: ParsedStreamingReply | null,
) {
  if (
    operation?.kind === "reply" &&
    !isPlannerOperationTerminal(operation.status) &&
    operation.assistantMessageId &&
    reply?.question
  ) {
    return operation.assistantMessageId;
  }
  return currentQuestionMessageId?.trim() || undefined;
}

function resolveActiveQuestion(
  messages: PlanningMessage[],
  activeQuestionId: string | undefined,
  operation: PlannerOperationState | null | undefined,
  reply: ParsedStreamingReply | null,
) {
  if (
    operation?.kind === "reply" &&
    !isPlannerOperationTerminal(operation.status) &&
    operation.assistantMessageId === activeQuestionId &&
    reply?.question
  ) {
    return reply.question;
  }
  return messages.find((message) => message.planning_message_id === activeQuestionId)?.question_payload;
}

function persistedAnswersByQuestionId(
  messages: PlanningMessage[],
  currentQuestionMessageId: string | undefined,
  operation: PlannerOperationState | null | undefined,
) {
  const answers: Record<string, AnswerPayload> = {};

  for (let index = 0; index < messages.length; index += 1) {
    const message = messages[index];
    if (message.role !== "assistant" || !message.question_payload) {
      continue;
    }
    const next = messages[index + 1];
    if (next?.role !== "user") {
      continue;
    }
    answers[message.planning_message_id] = next.answer_payload ?? { text: next.content_markdown };
  }

  if (
    currentQuestionMessageId &&
    operation?.kind === "reply" &&
    !isPlannerOperationTerminal(operation.status) &&
    operation.pendingReply?.answer
  ) {
    answers[currentQuestionMessageId] = operation.pendingReply.answer;
  }

  return answers;
}
