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
  isCanonical: boolean;
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
  // Resolve `parsedReply` once: prefer the caller's memoised value, fall back
  // to a single parse here if the caller did not supply one. The four call
  // sites below all reuse this resolution rather than re-running the parser.
  const reply: ParsedStreamingReply | null = parsedReply ?? (operation?.replyText
    ? parseStreamingQuestion(operation.replyText)
    : null);
  const renderedMessages = projectRenderedMessages(messages, operation);
  const resolvedAnswersByQuestionId = persistedAnswersByQuestionId(renderedMessages, currentQuestionMessageId, operation);
  const activeQuestionId = resolveActiveQuestionId(currentQuestionMessageId, operation, reply);
  const activeQuestion = resolveActiveQuestion(renderedMessages, activeQuestionId, operation, reply);

  const items: TranscriptItem[] = renderedMessages.map((message) => ({
    kind: "message",
    key: `message-${message.planning_message_id}`,
    message,
    isCanonical: true,
    resolvedAnswer: resolvedAnswersByQuestionId[message.planning_message_id],
    isInteractive:
      conversationState === "ready" &&
      message.role === "assistant" &&
      Boolean(message.question_payload) &&
      message.planning_message_id === activeQuestionId,
    variantCount: message.branch_count,
    currentVariantIndex: message.branch_index,
    activityRunId: message.role === "assistant" ? message.source_run_id : undefined,
  }));

  const sessionID = renderedMessages[renderedMessages.length - 1]?.planning_session_id ?? messages[messages.length - 1]?.planning_session_id ?? "";

  // Regenerate streams a new assistant branch; show its ghost while live (even
  // before text arrives, so the slot shows the thinking/feather), and keep
  // provisional output if a non-success terminal left partial text behind.
  if (
    operation?.kind === "regenerate" &&
    operation.status !== "done" &&
    (!isPlannerOperationTerminal(operation.status) || operation.replyText.trim() !== "")
  ) {
    const canonicalIds = new Set(renderedMessages.map((m) => m.planning_message_id));
    if (operation.assistantMessageId && !canonicalIds.has(operation.assistantMessageId) && operation.assistantCreatedAt) {
      const parsed = reply ?? parseStreamingQuestion(operation.replyText);
      items.push(buildGhostMessage({
        messageId: operation.assistantMessageId,
        parentMessageId: operation.parentMessageId,
        sessionID,
        role: "assistant",
        content: parsed.content,
        question: parsed.question ?? undefined,
        createdAt: operation.assistantCreatedAt,
        streaming: operation.status === "streaming" || operation.status === "pending",
        activityRunId: activeOperationRunId,
        activityItems: operation.activityItems,
      }));
    }
  }

  if (operation?.kind === "reply" && operation.status !== "done") {
    const canonicalIds = new Set(renderedMessages.map((m) => m.planning_message_id));

    if (operation.pendingReply && operation.userMessageId && !canonicalIds.has(operation.userMessageId) && operation.userCreatedAt) {
      items.push(buildGhostMessage({
        messageId: operation.userMessageId,
        parentMessageId: operation.parentMessageId,
        sessionID,
        role: "user",
        content: operation.pendingReply.contentMarkdown,
        answer: operation.pendingReply.answer,
        createdAt: operation.userCreatedAt,
      }));
    }

    if (operation.assistantMessageId && !canonicalIds.has(operation.assistantMessageId) && operation.assistantCreatedAt) {
      const parsed = reply ?? parseStreamingQuestion(operation.replyText);
      items.push(buildGhostMessage({
        messageId: operation.assistantMessageId,
        parentMessageId: operation.userMessageId,
        sessionID,
        role: "assistant",
        content: parsed.content,
        question: parsed.question ?? undefined,
        createdAt: operation.assistantCreatedAt,
        streaming: operation.status === "streaming" || operation.status === "pending",
        activityRunId: activeOperationRunId,
        activityItems: operation.activityItems,
      }));
    }
  }

  const itemsWithLiveOperation = attachLiveOperationState(items, operation, activeOperationRunId);

  return {
    items: itemsWithLiveOperation,
    activeQuestion,
    activeQuestionId,
  };
}

function projectRenderedMessages(
  messages: PlanningMessage[],
  operation: PlannerOperationState | null | undefined,
): PlanningMessage[] {
  if (!shouldProjectPendingBranch(messages, operation)) {
    return messages;
  }

  const anchorId = operation?.parentMessageId?.trim();
  if (!anchorId) {
    return [];
  }

  const anchorIndex = messages.findIndex((message) => message.planning_message_id === anchorId);
  if (anchorIndex < 0) {
    return messages;
  }

  const projected = messages.slice(0, anchorIndex + 1);
  if (operation?.kind === "reply") {
    return projected;
  }
  const canonicalAssistant = findCanonicalPendingAssistant(messages, operation);
  if (canonicalAssistant) {
    projected.push(canonicalAssistant);
  }
  return projected;
}

function shouldProjectPendingBranch(
  messages: PlanningMessage[],
  operation: PlannerOperationState | null | undefined,
): boolean {
  if (!operation || isPlannerOperationTerminal(operation.status)) {
    return false;
  }
  const anchorId = operation.parentMessageId?.trim();
  if (!anchorId) {
    return false;
  }
  if (operation.kind === "reply") {
    const pendingUserId = operation.userMessageId?.trim();
    const pendingAssistantId = operation.assistantMessageId?.trim();
    if (pendingUserId && messages.some((message) => message.planning_message_id === pendingUserId)) {
      return false;
    }
    return hasMessagesAfterAnchor(messages, anchorId, pendingAssistantId);
  }
  if (operation.kind === "regenerate") {
    const pendingAssistantId = operation.assistantMessageId?.trim();
    return hasMessagesAfterAnchor(messages, anchorId, pendingAssistantId);
  }
  return false;
}

function hasMessagesAfterAnchor(
  messages: PlanningMessage[],
  anchorId: string,
  canonicalAssistantId?: string,
): boolean {
  const anchorIndex = messages.findIndex((message) => message.planning_message_id === anchorId);
  if (anchorIndex < 0) {
    return false;
  }
  const trailing = messages.slice(anchorIndex + 1);
  if (trailing.length === 0) {
    return false;
  }
  if (!canonicalAssistantId) {
    return true;
  }
  return trailing.some((message) => message.planning_message_id !== canonicalAssistantId);
}

function findCanonicalPendingAssistant(
  messages: PlanningMessage[],
  operation: PlannerOperationState | null | undefined,
): PlanningMessage | undefined {
  const assistantId = operation?.assistantMessageId?.trim();
  if (!assistantId) {
    return undefined;
  }
  return messages.find((message) => message.planning_message_id === assistantId);
}

// buildGhostMessage produces an optimistic transcript item for an in-flight
// operation (the user message before the messages refetch lands, and the
// streaming assistant reply). It attaches under the given parent and defaults
// to a lone branch; authoritative branch metadata arrives on the next refetch.
function buildGhostMessage(args: {
  messageId: string;
  parentMessageId?: string;
  sessionID: string;
  role: PlanningMessage["role"];
  content: string;
  question?: QuestionPayload;
  answer?: AnswerPayload;
  createdAt: string;
  streaming?: boolean;
  activityRunId?: string;
  activityItems?: PlannerActivityItem[];
}): TranscriptItem {
  return {
    kind: "message",
    key: `message-${args.messageId}`,
    message: {
      planning_message_id: args.messageId,
      planning_session_id: args.sessionID,
      parent_message_id: args.parentMessageId,
      role: args.role,
      content_markdown: args.content,
      source_run_id: args.role === "assistant" ? args.activityRunId : undefined,
      created_at: args.createdAt,
      question_payload: args.question,
      answer_payload: args.answer,
      branch_index: 0,
      branch_count: 1,
    },
    resolvedAnswer: undefined,
    isCanonical: false,
    isInteractive: false,
    isStreaming: args.streaming,
    contentMarkdown: args.role === "assistant" ? args.content : undefined,
    activityRunId: args.streaming ? args.activityRunId : undefined,
    liveActivityItems: args.streaming ? args.activityItems : undefined,
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
