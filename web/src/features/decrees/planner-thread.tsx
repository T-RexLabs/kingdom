import { useEffect, useMemo, useRef } from "react";
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
import { plannerScope, plannerScopeKey, type PlannerDraftSubmission } from "./planner-workspace-state";
import { usePlannerSessionUI, useTrackedPlannerOperations } from "./use-planner-workspace-state";

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
  replyRecoveryPending,
  onRetryReplyRecovery,
  onSubmitReply,
  onRegenerate,
  onActivateVariant,
  stop,
  recoveryBlocked,
  recovery,
}: {
  session: PlanningSessionDetail;
  stop?: { available: boolean; pending: boolean; error?: string; onStop: () => Promise<void> };
  recoveryBlocked?: boolean;
  recovery?: { checking: boolean; onCheck: () => Promise<void>; pendingKind?: string | null; onRetry?: () => Promise<void> };
  operation?: PlannerOperationState | null;
  connection?: PlannerConnection;
  operationQueryState?: {
    isLoading: boolean;
    isError: boolean;
    isRetrying: boolean;
    error: unknown;
    retry: () => void;
  };
  replyRecoveryPending: boolean;
  onRetryReplyRecovery: () => Promise<void> | void;
  onSubmitReply: (payload: { text: string; answer?: AnswerPayload; parentMessageId?: string; draft?: PlannerDraftSubmission }) => Promise<void> | void;
  onRegenerate: (payload: { messageId: string; parentMessageId?: string }) => Promise<void> | void;
  onActivateVariant: (payload: { messageId: string }) => Promise<void> | void;
}) {
  const scope = plannerScope(session);
  const { store, state } = usePlannerSessionUI(scope);
  const messagePages = usePlannerMessagePages(session.planning_session_id);
  const { operations } = useTrackedPlannerOperations();
  const leaf = session.active_leaf_message_id ?? "root";
  const leafSourceRun = messagePages.messages.find((message) => message.planning_message_id === leaf)?.source_run_id;
  const continuation = operations.find((entry) => entry.workspace === scope.workspace && entry.sessionId === scope.sessionId && entry.draftBranch
    && (entry.operationId === session.current_run_id || entry.operationId === leafSourceRun));
  const branch = state.leafBranches[leaf] ?? continuation?.draftBranch ?? leaf;
  useEffect(() => {
    if (branch === leaf) return;
    store.updateSession({ workspace: scope.workspace, sessionId: scope.sessionId }, (current) => current.leafBranches[leaf] === branch ? current : { ...current, leafBranches: { ...current.leafBranches, [leaf]: branch } });
  }, [store, scope.workspace, scope.sessionId, leaf, branch]);
  const draft = store.draft(scope, branch);
  const inputText = draft.text;
  const editingMessageId = session.conversation_state === "ready" ? draft.editing?.messageId ?? null : null;
  const setInputText = (text: string) => store.setText(scope, branch, text);
  const draftIdentity = `${plannerScopeKey(scope)}:${branch}`;
  const retryingReplyRecovery = state.request?.kind === "reply" && state.request.phase === "submitting";
  const retryingSessions = useRef(new Set<string>());
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

  const sendingRef = useRef(new Set<string>());
  const requestFollowLatest = () => store.updateSession(scope, (current) => ({ ...current, followLatestRequest: current.followLatestRequest + 1 }));

  const sendText = async (text: string, answer?: AnswerPayload, parentMessageId?: string, draftKind?: "composer" | "edit") => {
    if (replyRecoveryPending || recoveryBlocked || recovery?.pendingKind || sendingRef.current.has(draftIdentity)) return;
    const trimmed = text.trim();
    if (!trimmed) return;
    const generation = draftKind === "edit" ? draft.editing?.generation : draft.generation;
    sendingRef.current.add(draftIdentity);
    try {
      await onSubmitReply({ text: trimmed, answer, parentMessageId, ...(draftKind && generation !== undefined ? { draft: { branch, generation, kind: draftKind } } : {}) });
      requestFollowLatest();
      if (draftKind && generation !== undefined) store.clearSubmittedDraft(scope, branch, generation, draftKind === "edit");
    } catch {
      // The controller owns recovery; retain the originating draft, including edits.
    } finally {
      sendingRef.current.delete(draftIdentity);
    }
  };

  const handleRetryReplyRecovery = async () => {
    if (!replyRecoveryPending || retryingSessions.current.has(draftIdentity)) return;
    retryingSessions.current.add(draftIdentity);
    try {
      await onRetryReplyRecovery();
      requestFollowLatest();
    } catch {
      // The planner page surfaces the mutation error; the composer draft stays untouched.
    } finally {
      retryingSessions.current.delete(draftIdentity);
    }
  };

  const handleEditMessage = (messageId: string) => {
    const item = transcript.items.find((candidate) => candidate.message.planning_message_id === messageId);
    if (item) store.updateDraft(scope, branch, (current) => ({ ...current, editing: { messageId, text: item.message.content_markdown, generation: (current.editing?.generation ?? 0) + 1 } }));
  };

  const handleEditSubmit = (text: string) => {
    const editingItem = transcript.items.find((item) => item.message.planning_message_id === editingMessageId);
    if (!editingItem) return;
    // Editing branches a new sibling off the edited message's parent.
    void sendText(text, undefined, editingItem.message.parent_message_id, "edit");
  };

  const handleEditCancel = () => {
    store.updateDraft(scope, branch, (current) => ({ ...current, editing: undefined }));
  };

  const handleRegenerate = async (messageId: string, parentMessageId?: string) => {
    try {
      await onRegenerate({ messageId, parentMessageId });
      requestFollowLatest();
    } catch { /* The session controller exposes the retained request and error. */ }
  };

  const handleActivateVariant = (messageId: string) => {
    void Promise.resolve(onActivateVariant({ messageId })).catch(() => {});
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
          key={plannerScopeKey(scope)}
          followLatestRequest={state.followLatestRequest}
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
            editText: draft.editing?.text,
            onEditChange: (text) => store.updateDraft(scope, branch, (current) => current.editing ? { ...current, editing: { ...current.editing, text, generation: current.editing.generation + 1 } } : current),
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

      {state.storageUnavailable ? <p role="status" className="mx-4 mt-2 text-sm text-foreground-subtle">Browser storage is unavailable. Drafts are kept only while this page remains open.</p> : null}
      {recoveryBlocked ? <p role="alert" className="mx-4 mt-2 text-sm text-status-failed">A previous planner request has an unresolved outcome from an earlier browser session. Your draft is preserved. Sending and retrying are blocked until that request can be verified. {recovery ? <button type="button" disabled={recovery.checking} onClick={() => void recovery.onCheck()} className="ml-2 underline">{recovery.checking ? "Checking..." : "Check Previous Request"}</button> : null}</p> : null}
      {stop?.available ? <div className="mx-4 mt-2 flex items-center gap-2">
        <button type="button" disabled={stop.pending || recoveryBlocked} onClick={() => void stop.onStop().catch(() => {})} className="rounded-md border border-border px-3 py-1.5 text-sm disabled:opacity-50">{stop.pending ? "Stopping..." : "Stop"}</button>
        {stop.error ? <span role="alert" className="text-sm text-status-failed">{stop.error}</span> : null}
      </div> : null}
      {recovery?.pendingKind && recovery.pendingKind !== "reply" && recovery.pendingKind !== "stop" && !recoveryBlocked ? <div role="status" className="mx-4 mt-2 text-sm text-foreground-subtle">
        A previous planner request could not be confirmed. Your draft is preserved.
        <button type="button" onClick={() => void recovery.onRetry?.().catch(() => {})} className="ml-2 underline">Retry Previous Request</button>
      </div> : null}
      {replyRecoveryPending && !recoveryBlocked ? (
        <div
          role="status"
          className="mx-4 mt-2 flex items-center gap-3 rounded-xl border border-border/40 bg-surface-raised/40 p-3 text-sm text-foreground-subtle"
        >
          <span className="min-w-0 flex-1">
            A previous reply could not be confirmed. Retry it with the original request and idempotency key before sending a different reply. Your current draft will be preserved.
          </span>
          <button
            type="button"
            className="shrink-0 rounded-md border border-border/60 bg-surface-raised px-2.5 py-1 text-xs font-medium text-foreground transition-colors hover:bg-surface-overlay disabled:cursor-not-allowed disabled:opacity-60"
            disabled={retryingReplyRecovery}
            onClick={() => void handleRetryReplyRecovery()}
          >
            {retryingReplyRecovery ? "Retrying..." : "Retry Previous Reply"}
          </button>
        </div>
      ) : null}

      <div className="shrink-0 border-t border-border/50 bg-surface/80 px-4 py-3">
        <PlannerComposer
          value={inputText}
          disabled={composerDisabled}
          sendDisabled={replyRecoveryPending || recoveryBlocked || Boolean(recovery?.pendingKind)}
          textareaRef={composerTextareaRef}
          onChange={setInputText}
          onSend={() => {
            void sendText(inputText, inputText.trim() ? { text: inputText.trim() } : undefined, undefined, "composer");
          }}
        />
      </div>
    </div>
  );
}
