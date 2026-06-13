import {
  newIdempotencyKey,
  usePlanningOperation,
  usePlanningSession,
  usePlanningSessionDraft,
  useUpdatePlanningSessionModelSelection,
  useUpdatePlanningSessionTitle,
} from "@/api/hooks";
import { useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { Feather, FileText, Sparkles } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ModelSelectField } from "./model-select-field";
import { getPlanningSessionDisplayTitleValue, hasPlaceholderTitle } from "./planning-session-title";
import { ThinkingEffortField } from "./thinking-effort-field";
import { PlannerThread } from "./planner-thread";
import { PlanResultModal } from "./planner-draft-panel";
import { ProviderModelCatalogNotice, useProviderModelCatalogController } from "./provider-model-catalog-state";
import { usePlannerMessagePages } from "./use-planner-message-pages";
import { isPlannerOperationTerminal } from "./planner-operation-controller";
import { usePlanningSessionTitleController } from "./use-planning-session-title-controller";
import { usePlannerSessionController } from "./use-planner-session-controller";
import { useGlobalFormalize } from "./use-global-formalize";
import { usePlannerOperation } from "./use-planner-operation";
import { usePlanningSessionAutoTitleCatchup } from "./use-planning-session-auto-title-catchup";
import {
  applyPlanningSessionFields,
  compareAndSwapPlanningSessionTitle,
} from "./planner-session-cache";
import { useQueryClient } from "@tanstack/react-query";
import { normalizeThinkingEffortSelection } from "./provider-model-selection";

export function PlanningSessionPage() {
  const { sessionId } = useParams({ from: "/decrees/planning/$sessionId" });
  const { show_plan: showPlanParam } = useSearch({ from: "/decrees/planning/$sessionId" });
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const sessionQuery = usePlanningSession(sessionId);
  const session = sessionQuery.data;
  const updateTitle = useUpdatePlanningSessionTitle(sessionId);
  const updateModelSelection = useUpdatePlanningSessionModelSelection(sessionId);
  // Destructure stable handles up front. The hook object is recreated
  // each render, but `mutate`, `reset`, and `error` are stable across
  // renders (TanStack Query guarantee). Using these in effect deps
  // means cache patches that produce a new session reference don't
  // restart the autosave debounce timer.
  const {
    mutate: modelMutate,
    reset: modelReset,
    error: modelError,
	} = updateModelSelection;
	const providerModelCatalogController = useProviderModelCatalogController();
	const providerModelCatalog = providerModelCatalogController.state;
	const messagePages = usePlannerMessagePages(sessionId);
  const userMessageCount = messagePages.messages.filter((m) => m.role === "user").length;
  const [model, setModel] = useState("");
  const [thinkingEffort, setThinkingEffort] = useState("");
  const [showPlanModal, setShowPlanModal] = useState(false);
  const { activeOperation, activeOperationId, connection, setActiveOperationId, submitReply, startFormalize, createOperation, regenerate, activateVariant } =
    usePlannerSessionController(session);
  const { current: globalFormalize, trackFormalize } = useGlobalFormalize();
  const draftQuery = usePlanningSessionDraft(showPlanModal || session?.current_draft_id ? sessionId : undefined);
  const titleOperationQuery = usePlanningOperation(session?.current_title_run_id);
  usePlannerOperation(titleOperationQuery.data ?? null);
  usePlanningSessionAutoTitleCatchup(sessionId, session);

  // Title persistence is rooted in the explicit commit event (Enter,
  // blur with no error, or Retry). The save is the consequence of the
  // commit, not a debounced diff against canonical — so the optimistic
  // cache write cannot suppress the PATCH the way the previous
  // diff-based effect did. On mutation failure we CAS-rollback only the
  // title slice (so concurrent SSE updates to other fields and a later
  // successful title save survive), then expose the error inline.
  // commitTitle re-throws so the page-owned commit funnel can decide
  // whether to exit edit mode (success) or stay in edit mode with the
  // failed draft visible (failure). The retry chip reads the *live*
  // titleInput at click time — the trimmed value no longer needs to be
  // captured on the error.
  const [titleSaveError, setTitleSaveError] = useState<{ message: string } | null>(null);
  const commitTitle = useCallback(
    async (trimmed: string): Promise<void> => {
      if (!session) return;
      const previousSession = session;
      // Field-scoped optimistic write via the shared helper. Patches
      // detail + every list slice with only title + title_state.
      // Spreading a closure-captured `session` would clobber any
      // concurrently-applied SSE updates (e.g. current_run_id
          // from a parallel formalize) AND would change the session
      // reference enough to refire the model-sync useEffect, which
      // would reset the user's in-progress model dropdown selection
      // back to server-canonical state. Field-scoped here, narrow
      // model-sync deps below — both halves of the same fix.
      applyPlanningSessionFields(queryClient, sessionId, {
        title: trimmed,
        title_state: "custom",
      });
      try {
        await updateTitle.mutateAsync({
          title: trimmed,
          key: newIdempotencyKey("planning-update-title"),
        });
        setTitleSaveError(null);
      } catch (err) {
        // CAS rollback: revert iff the cache still holds *exactly* the
        // optimistic write we made. If a successful concurrent save or
        // an SSE patch has since changed the title or title_state, the
        // cache reflects newer truth — don't clobber it.
        compareAndSwapPlanningSessionTitle(
          queryClient,
          sessionId,
          trimmed,
          "custom",
          previousSession.title,
          previousSession.title_state,
        );
        const message = err instanceof Error ? err.message : "Could not save title";
        setTitleSaveError({ message });
        throw err;
      }
    },
    [session, sessionId, updateTitle, queryClient],
  );

  const {
    displayTitle,
    isEditing: isEditingTitle,
    titleInput,
    isRevealing: isRevealingTitle,
    startEditing,
    exitEdit,
    abandonEdit,
    setTitleInput,
  } = usePlanningSessionTitleController({
    sessionId,
    session,
  });

  // Single commit funnel for Enter / blur (when no error) / Retry. The
  // controller no longer fires commits internally — it owns edit-state
  // only — so handleCommit reads `titleInput` (always the live value) at
  // click time. On success the page exits edit mode; on failure we stay
  // in edit mode so the failed draft remains visible and the user can
  // keep typing → Retry or click Discard. A no-op commit (empty or
  // unchanged) takes the abandon-edit path when an error chip is
  // showing (so the chip clears) and the plain exit-edit path otherwise.
  const handleCommit = useCallback(async () => {
    if (!session) return;
    const trimmed = titleInput.trim();
    if (trimmed === "" || trimmed === session.title) {
      if (titleSaveError) {
        abandonEdit(session.title);
        setTitleSaveError(null);
      } else {
        exitEdit();
      }
      return;
    }
    try {
      await commitTitle(trimmed);
      exitEdit();
    } catch {
      // commitTitle has already CAS-rolled-back the cache and set
      // titleSaveError. Stay in edit mode so the failed draft is
      // visible — Retry uses the live input, Discard uses abandonEdit.
    }
  }, [session, titleInput, titleSaveError, commitTitle, exitEdit, abandonEdit]);

  // Reseed local model + thinking-effort state ONLY when the
  // server-canonical model_selection actually changes (or we navigate
  // to a different session). Depending on the whole `session` object
  // would refire on every cache update — including a successful title
  // save's onSuccess writing the full server response into the cache —
  // and overwrite the user's in-progress model selection before the
  // 500ms autosave fires. Narrow string deps mean reference changes
  // alone don't trigger the reseed.
  useEffect(() => {
    if (!session) {
      return;
    }
    setModel(session.model_selection.model ?? "");
    setThinkingEffort(session.model_selection.thinking_effort ?? "");
  }, [sessionId, session?.model_selection.model, session?.model_selection.thinking_effort]);

  // Re-hydrate activeOperationId from global tracker when returning to this page mid-generation.
  // Only re-hydrate when the session is still processing — once the streamed operation reaches
  // a terminal state, the controller clears its local activeOperationId.
  useEffect(() => {
    if (
      globalFormalize &&
      globalFormalize.sessionId === sessionId &&
      !activeOperationId &&
      session?.conversation_state === "processing"
    ) {
      setActiveOperationId(globalFormalize.operationId);
    }
  }, [globalFormalize, sessionId, activeOperationId, setActiveOperationId, session?.conversation_state]);

	const selectedModel = useMemo(
		() => providerModelCatalog.items.find((item) => item.id === model),
		[model, providerModelCatalog.items]
  );

  useEffect(() => {
    if (!model.trim()) return;
    setThinkingEffort((current) =>
      normalizeThinkingEffortSelection(selectedModel, current, { preserveWhenModelMissing: true })
    );
  }, [model, selectedModel]);

  const hasModelSelectionChanges =
    Boolean(session) &&
    (model !== (session?.model_selection.model ?? "") ||
      thinkingEffort !== (session?.model_selection.thinking_effort ?? ""));

  const hasUserMessages = userMessageCount > 0;
  const formalizePending = activeOperation?.kind === "formalize" && !isPlannerOperationTerminal(activeOperation.status);
  const hasPlan = Boolean(session?.current_draft_id);
  const showPlanConsumedForSession = useRef<string>("");

  // Auto-open plan modal when arriving via ?show_plan=true (e.g. from notification "View Plan" link)
  useEffect(() => {
    if (!showPlanParam) {
      showPlanConsumedForSession.current = "";
      return;
    }
    if (hasPlan && showPlanConsumedForSession.current !== sessionId) {
      showPlanConsumedForSession.current = sessionId;
      setShowPlanModal(true);
      void navigate({
        to: "/decrees/planning/$sessionId",
        params: { sessionId },
        search: {},
        replace: true,
      });
    }
  }, [showPlanParam, hasPlan, navigate, sessionId]);

  // Debounced model-selection autosave. .mutate() is the right
  // fire-and-forget shape: errors land in `modelError` (driving the
  // dedicated chip below) without leaking unhandled promise
  // rejections, and the supersede pattern in useSlicedMutation
  // ensures rapid edits don't let an older response clobber a newer
  // one's applied state.
  //
  // Deps deliberately exclude `session`. `hasModelSelectionChanges`
  // already gates on the canonical-vs-local comparison (and on
  // `Boolean(session)`), so any cache patch that changes the session
  // reference without changing the actual canonical model_selection
  // — sibling SSE updates, terminal events, title saves writing the
  // server response back into the cache — must NOT restart this
  // 500ms timer. With `session` in deps, rapid SSE traffic could
  // starve the autosave indefinitely.
  useEffect(() => {
    if (!hasModelSelectionChanges) return;
    const timer = setTimeout(() => {
      modelMutate({
        modelSelection: model.trim()
          ? {
              model: model.trim(),
              ...(thinkingEffort.trim() ? { thinking_effort: thinkingEffort.trim() } : {}),
            }
          : undefined,
        key: newIdempotencyKey("planning-update-model"),
      });
    }, 500);
    return () => clearTimeout(timer);
  }, [model, thinkingEffort, hasModelSelectionChanges, modelMutate]);

  // When the user reverts the dropdown back to canonical, any prior
  // failure is now meaningless — the chip's Retry would re-send the
  // canonical value and a stale error would survive subsequent edits
  // until the next attempt's onMutate fired. Calling reset() clears
  // the mutation state so future dirty edits start clean.
  useEffect(() => {
    if (!hasModelSelectionChanges && modelError) {
      modelReset();
    }
  }, [hasModelSelectionChanges, modelError, modelReset]);

  if (sessionQuery.isLoading) {
    return (
      <div className="flex h-full items-center justify-center rounded-xl border border-border/80 bg-surface">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-gold border-t-transparent" />
      </div>
    );
  }
  if (sessionQuery.isError || !session) {
    const message = sessionQuery.error instanceof Error
      ? sessionQuery.error.message
      : "Planning session not found.";
    return (
      <div className="flex h-full items-center justify-center rounded-xl border border-border/80 bg-surface">
        <div className="max-w-md space-y-3 rounded-md border border-status-failed/30 bg-status-failed/10 p-4 text-center">
          <p className="text-sm font-medium text-foreground">Could not load planning session</p>
          <p className="text-xs text-foreground-subtle">{message}</p>
          <button
            type="button"
            onClick={() => { void sessionQuery.refetch(); }}
            className="rounded-md border border-border bg-background px-3 py-1.5 text-sm font-medium text-foreground transition-colors hover:bg-surface-raised/60"
          >
            Try again
          </button>
        </div>
      </div>
    );
  }

  const handleFormalizePlan = async () => {
    const operation = await startFormalize();
    setActiveOperationId(operation.run_id);
    trackFormalize(sessionId, operation.run_id, displayTitle);
  };

	const visibleDisplayTitle = getPlanningSessionDisplayTitleValue(displayTitle, "New session");
	const isPlaceholderHeaderTitle = Boolean(session) && hasPlaceholderTitle(session) && displayTitle.trim().length === 0;

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden rounded-xl border border-border/80 bg-surface">
      <div className="relative z-20 shrink-0 space-y-3 border-b border-border/50 bg-surface-raised/20 px-4 py-3">
        <div className="flex items-center justify-between gap-3">
          <div className="flex min-w-0 flex-1 items-center gap-2">
            <Sparkles className="h-4 w-4 shrink-0 text-gold/70" />
            {isEditingTitle ? (
              <input
                autoFocus
                type="text"
                value={titleInput}
                onChange={(e) => setTitleInput(e.target.value)}
                // Don't blur-commit while a save error is showing — the
                // user is mid-recovery and a re-blur would re-fire the
                // commit before they got to retry/discard.
                onBlur={titleSaveError ? undefined : () => { void handleCommit(); }}
                onKeyDown={(e) => {
                  if (e.key === "Enter") void handleCommit();
                }}
                placeholder="Session title..."
                className={`flex-1 border-b ${titleSaveError ? "border-status-failed" : "border-gold"} bg-transparent text-sm font-semibold text-foreground placeholder:text-foreground-subtle/50 focus:outline-none`}
              />
            ) : (
					<button
						type="button"
						className="group/title flex items-center gap-1.5 truncate text-left"
						onClick={startEditing}
						aria-label="Edit session title"
					>
                {isRevealingTitle ? (
                  <span className="flex items-center gap-[0.25em] text-sm font-semibold text-foreground">
                    {displayTitle.split(/\s+/).filter(Boolean).map((word, i) => (
                      <span
                        key={i}
                        className="inline-block opacity-0"
                        style={{
                          animation: "title-word-reveal 250ms ease-out forwards",
                          animationDelay: `${i * 100}ms`,
                        }}
                      >
                        {word}
                      </span>
                    ))}
                  </span>
						) : (
							<>
								<span
									className={`truncate text-sm font-semibold ${isPlaceholderHeaderTitle ? "italic text-foreground-subtle" : "text-foreground"}`}
								>
									{visibleDisplayTitle}
								</span>
								<Feather className="h-3 w-3 shrink-0 text-foreground-subtle/0 transition-colors group-hover/title:text-foreground-subtle" />
							</>
                )}
              </button>
            )}
            {titleSaveError ? (
              <div
                role="alert"
                className="ml-2 flex shrink-0 items-center gap-2 rounded-md border border-status-failed/30 bg-status-failed/10 px-2 py-1 text-xs text-status-failed"
              >
                <span className="truncate" title={titleSaveError.message}>
                  Couldn’t save: {titleSaveError.message}
                </span>
                <button
                  type="button"
                  className="rounded border border-status-failed/40 bg-status-failed/15 px-2 py-0.5 font-medium hover:bg-status-failed/25"
                  onClick={() => { void handleCommit(); }}
                >
                  Retry
                </button>
                <button
                  type="button"
                  className="rounded border border-border/40 bg-background/40 px-2 py-0.5 font-medium text-foreground-subtle hover:bg-surface-raised/40"
                  onClick={() => {
                    setTitleSaveError(null);
                    abandonEdit(session.title);
                  }}
                >
                  Discard
                </button>
              </div>
            ) : null}
          </div>

          <div className="flex shrink-0 items-center gap-2">
            {hasPlan ? (
              <button
                type="button"
                onClick={() => setShowPlanModal(true)}
                className="flex items-center gap-1.5 rounded-md border border-gold/30 bg-gold/5 px-2.5 py-1.5 text-xs font-medium text-gold transition-colors hover:bg-gold/15"
              >
                <FileText className="h-3 w-3" />
                View Plan
              </button>
            ) : null}
            <button
              type="button"
              onClick={() => void handleFormalizePlan()}
              disabled={formalizePending || !hasUserMessages || session.conversation_state !== "ready"}
              className="flex items-center gap-1.5 rounded-md border border-gold/50 bg-gold/10 px-3 py-1.5 text-xs font-semibold text-gold transition-all hover:bg-gold/20 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {formalizePending ? (
                <div className="h-3 w-3 animate-spin rounded-full border-2 border-gold border-t-transparent" />
              ) : (
                <Sparkles className="h-3 w-3" />
              )}
              {formalizePending ? "Generating Plan..." : "Generate Plan"}
            </button>
          </div>
        </div>

        <div className="space-y-3 border-t border-border/60 pt-3">
          <div className="flex flex-wrap items-end gap-3">
            <div className="w-full">
              <ProviderModelCatalogNotice
                state={providerModelCatalog}
				onRetry={providerModelCatalogController.retry}
				retryPending={providerModelCatalogController.retryPending}
              />
            </div>
            <div className="w-[280px]">
              <ModelSelectField
                id="planning-model"
                label="Planner Model"
                value={model}
				loading={providerModelCatalog.isLoading}
                disabled={providerModelCatalog.disableSelection}
                options={providerModelCatalog.items}
                triggerPlaceholder={providerModelCatalog.triggerPlaceholder}
                emptyStateLabel={providerModelCatalog.emptyStateLabel}
                onChange={(next) => {
                  setModel(next);
                  const nextModel = providerModelCatalog.items.find((item) => item.id === next);
                  setThinkingEffort(normalizeThinkingEffortSelection(nextModel, "", { preserveWhenModelMissing: true }));
                }}
              />
            </div>
            <div className="w-[240px]">
              <ThinkingEffortField
                id="planning-thinking-effort"
                label="Thinking Effort"
                value={thinkingEffort}
                model={selectedModel}
                disabled={providerModelCatalog.disableSelection}
                onChange={setThinkingEffort}
              />
            </div>
          </div>
          {hasModelSelectionChanges && modelError ? (
            <div
              role="alert"
              className="flex w-full items-center gap-2 rounded-md border border-status-failed/30 bg-status-failed/10 px-2 py-1 text-xs text-status-failed"
            >
              <span className="truncate" title={modelError.message}>
                Couldn’t save model selection: {modelError.message}
              </span>
              <button
                type="button"
                className="ml-auto rounded border border-status-failed/40 bg-status-failed/15 px-2 py-0.5 font-medium hover:bg-status-failed/25"
                onClick={() => {
                  modelMutate({
                    modelSelection: model.trim()
                      ? {
                          model: model.trim(),
                          ...(thinkingEffort.trim() ? { thinking_effort: thinkingEffort.trim() } : {}),
                        }
                      : undefined,
                    key: newIdempotencyKey("planning-update-model"),
                  });
                }}
              >
                Retry
              </button>
            </div>
          ) : null}
        </div>
      </div>

      {/* Session-load errors are handled by the early-return shell above. */}
      {createOperation.error ? (
        <div className="mx-4 mt-2 rounded-lg border border-status-failed/30 bg-status-failed/10 p-2.5 text-xs text-status-failed">
          {createOperation.error.message}
        </div>
      ) : null}

      <PlannerThread
        session={session}
        operation={activeOperation}
        connection={connection}
        onSubmitReply={({ text, answer, parentMessageId }) =>
          submitReply({ text, answer, parentMessageId })
        }
        onRegenerate={({ messageId, parentMessageId }) => regenerate({ messageId, parentMessageId })}
        onActivateVariant={({ messageId }) => activateVariant({ messageId })}
      />

      <PlanResultModal
        open={showPlanModal}
        draft={draftQuery.data ?? null}
        sessionId={sessionId}
        contextSnapshotId={session.context_snapshot_id ?? undefined}
        operation={activeOperation?.kind === "formalize" ? activeOperation : null}
        onClose={() => setShowPlanModal(false)}
      />
    </div>
  );
}
