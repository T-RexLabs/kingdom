import { useEffect, useMemo, useRef, useState } from "react";
import type { PlanningSessionDetail } from "@/api/hooks";
import { PlannerTranscript } from "./planner-transcript";
import { PlannerComposer } from "./planner-composer";
import { usePlannerMessagePages } from "./use-planner-message-pages";
import { buildPlannerTranscriptViewModel } from "./planner-transcript-view-model";
import { parseStreamingQuestion } from "./parse-streaming-question";
import {
  type AnswerPayload,
  type PlannerOperationState,
} from "./planner-operation-controller";
import type { PlannerConnection } from "./use-planner-operation";

function buildAnswerPayloadForOption(option: { label: string; value: string }): AnswerPayload {
  return {
    value: option.value,
    text: option.label,
  };
}

function buildAnswerPayloadForOptions(options: Array<{ label: string; value: string }>): AnswerPayload {
  return {
    values: options.map((option) => option.value),
    text: options.map((option) => option.label).join(", "),
  };
}

export function PlannerThread({
  session,
  operation,
  connection,
  operationQueryState,
  onSubmitReply,
  onRegenerate,
  onActivateVariant,
}: {
  session: PlanningSessionDetail;
  operation?: PlannerOperationState | null;
  connection?: PlannerConnection;
  operationQueryState?: {
    isLoading: boolean;
    isError: boolean;
    isRetrying: boolean;
    error: unknown;
    retry: () => void;
  };
  onSubmitReply: (payload: { text: string; answer?: AnswerPayload; parentMessageId?: string }) => Promise<void> | void;
  onRegenerate: (payload: { messageId: string; parentMessageId?: string }) => Promise<void> | void;
  onActivateVariant: (payload: { messageId: string }) => Promise<void> | void;
}) {
  const [inputText, setInputText] = useState("");
  const [editingMessageId, setEditingMessageId] = useState<string | null>(null);
  const [followLatestRequest, setFollowLatestRequest] = useState(0);
  const messagePages = usePlannerMessagePages(session.planning_session_id);
  const composerTextareaRef = useRef<HTMLTextAreaElement>(null);

  // Parse the growing reply text exactly once per delta, not four times per
  // build (once each in: regenerate branch, reply branch, active-question id,
  // active question). The view model accepts the parsed value as a hint and
  // falls back to lazy parsing only when no operation is in flight.
  const parsedReply = useMemo(
    () => (operation?.replyText ? parseStreamingQuestion(operation.replyText) : null),
    [operation?.replyText],
  );

  const transcript = useMemo(
    () =>
      buildPlannerTranscriptViewModel({
        messages: messagePages.messages,
        currentQuestionMessageId: session.active_leaf_message_id,
        conversationState: session.conversation_state,
        operation,
        parsedReply,
      }),
    [messagePages.messages, session.active_leaf_message_id, session.conversation_state, operation, parsedReply],
  );

  const messagesError = messagePages.error;
  const operationError = operation?.error ?? null;
  // Transport health is separate from canonical query health and from a
  // server-attested operation failure.
  const transportFailing =
    !operation || (operation.status !== "done" && operation.status !== "error" && operation.status !== "cancelled")
      ? connection?.state === "failed" || connection?.state === "retrying"
      : false;
  // `cancelled` is a system event (server shutdown), not a planner
  // failure. Render it as a neutral notice instead of a red error chip
  // so the user reads it the way it actually means it.
  const isCancelled = operation?.status === "cancelled";
  const isLoading = messagePages.isLoading;
  const composerDisabled = session.conversation_state !== "ready";

  // Clear editing state when an operation starts
  useEffect(() => {
    if (session.conversation_state === "processing") {
      setEditingMessageId(null);
    }
  }, [session.conversation_state]);

  const sendingRef = useRef(false);

  const sendText = async (text: string, answer?: AnswerPayload, parentMessageId?: string) => {
    if (sendingRef.current) return;
    const trimmed = text.trim();
    if (!trimmed) return;
    sendingRef.current = true;
    try {
      await onSubmitReply({ text: trimmed, answer, parentMessageId });
      setFollowLatestRequest((current) => current + 1);
      // Only clear the composer once the mutation has resolved successfully.
      // If onSubmitReply throws (e.g. network failure), the controller has
      // already restored the snapshot — keep the user's typed text in the
      // box so they can retry without re-typing.
      setInputText("");
    } finally {
      sendingRef.current = false;
    }
  };

  const handleEditMessage = (messageId: string) => {
    setEditingMessageId(messageId);
  };

  const handleEditSubmit = (text: string) => {
    const editingItem = transcript.items.find((item) => item.message.planning_message_id === editingMessageId);
    if (!editingItem) return;
    setEditingMessageId(null);
    // Editing branches a new sibling off the edited message's parent.
    void sendText(text, undefined, editingItem.message.parent_message_id);
  };

  const handleEditCancel = () => {
    setEditingMessageId(null);
  };

  const handleRegenerate = async (messageId: string, parentMessageId?: string) => {
    await onRegenerate({ messageId, parentMessageId });
    setFollowLatestRequest((current) => current + 1);
  };

  const handleActivateVariant = (messageId: string) => {
    void onActivateVariant({ messageId });
  };

  const handleTypeYourAnswer = (questionId: string) => {
    if (questionId !== transcript.activeQuestionId || composerDisabled) return;
    const composer = composerTextareaRef.current;
    if (!composer || composer.disabled) return;
    composer.focus({ preventScroll: true });
    const caret = composer.value.length;
    composer.setSelectionRange(caret, caret);
  };

  return (
    <div className="flex flex-1 min-h-0 flex-col">
      {messagesError ? (
        <div className="mx-4 mt-4 rounded-xl border border-status-failed/30 bg-status-failed/10 p-3 text-sm text-status-failed">
          {messagesError.message}
        </div>
      ) : null}

      {isLoading ? (
        <div className="flex flex-1 flex-col items-center justify-center gap-3 py-12">
          <div className="h-6 w-6 animate-spin rounded-full border-2 border-gold border-t-transparent" />
          <p className="text-sm text-foreground-subtle">Loading conversation...</p>
        </div>
      ) : (
        <PlannerTranscript
          items={transcript.items}
          firstItemIndex={messagePages.firstItemIndex}
          totalCount={messagePages.totalCount}
          hasPreviousPage={messagePages.hasPreviousPage}
          isFetchingPreviousPage={messagePages.isFetchingPreviousPage}
          onLoadPrevious={() => {
            void messagePages.fetchPreviousPage();
          }}
          followLatestRequest={followLatestRequest}
          callbacks={{
            onSelect: (questionId, option) => {
              if (questionId !== transcript.activeQuestionId) return;
              void sendText(option.label, buildAnswerPayloadForOption(option));
            },
            onSubmitMultiSelect: (questionId, options) => {
              if (questionId !== transcript.activeQuestionId || options.length === 0) return;
              void sendText(options.map((option) => option.label).join(", "), buildAnswerPayloadForOptions(options));
            },
            onTypeYourAnswer: handleTypeYourAnswer,
            onEditMessage: handleEditMessage,
            onEditSubmit: handleEditSubmit,
            onEditCancel: handleEditCancel,
            onRegenerate: (messageId, parentMessageId) => {
              void handleRegenerate(messageId, parentMessageId);
            },
            onActivateVariant: handleActivateVariant,
            sessionId: session.planning_session_id,
            isProcessing: composerDisabled,
            editingMessageId,
          }}
        />
      )}

      {transportFailing ? (
        <div
          role="status"
          className="mx-4 mt-2 rounded-xl border border-border/40 bg-surface-raised/40 p-3 text-sm text-foreground-subtle"
        >
          Live updates disconnected. Kingdom is checking the operation in the background.
        </div>
      ) : null}

      {operationQueryState?.isLoading ? (
        <div
          role="status"
          className="mx-4 mt-2 rounded-xl border border-border/40 bg-surface-raised/40 p-3 text-sm text-foreground-subtle"
        >
          Checking planner operation...
        </div>
      ) : null}

      {operationQueryState?.isError ? (
        <div
          role="alert"
          className="mx-4 mt-2 flex items-center gap-3 rounded-xl border border-status-failed/30 bg-status-failed/10 p-3 text-sm text-status-failed"
        >
          <span className="min-w-0 flex-1">
            Could not refresh planner operation. Kingdom will retry automatically.
            {operationQueryState.error instanceof Error ? ` ${operationQueryState.error.message}` : ""}
          </span>
          <button
            type="button"
            className="shrink-0 rounded-md border border-status-failed/40 bg-status-failed/10 px-2.5 py-1 text-xs font-medium transition-colors hover:bg-status-failed/20 disabled:cursor-not-allowed disabled:opacity-60"
            disabled={operationQueryState.isRetrying}
            onClick={operationQueryState.retry}
          >
            {operationQueryState.isRetrying ? "Retrying..." : "Retry"}
          </button>
        </div>
      ) : null}

      {operationError ? (
        <div
          role="alert"
          className={
            isCancelled
              ? "mx-4 mt-2 rounded-xl border border-border/40 bg-surface-raised/40 p-3 text-sm text-foreground-subtle"
              : "mx-4 mt-2 rounded-xl border border-status-failed/30 bg-status-failed/10 p-3 text-sm text-status-failed"
          }
        >
          {operationError}
        </div>
      ) : null}

      <div className="shrink-0 border-t border-border/50 bg-surface/80 px-4 py-3">
        <PlannerComposer
          value={inputText}
          disabled={composerDisabled}
          textareaRef={composerTextareaRef}
          onChange={setInputText}
          onSend={() => {
            void sendText(inputText, inputText.trim() ? { text: inputText.trim() } : undefined);
          }}
        />
      </div>
    </div>
  );
}
