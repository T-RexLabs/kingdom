import {
  newIdempotencyKey,
  usePlanningOperation,
  usePlanningSession,
  usePlanningSessionDraft,
  useUpdatePlanningSessionModelSelection,
  useUpdatePlanningSessionTitle,
  type PlanningSessionDetail,
} from "@/api/hooks";
import { apiFetch } from "@/api/client";
import type { components } from "@/api/schema";
import { externalSourceFailureGuidance, externalSourceStatusLabel } from "@/features/connections/external-source-status";
import { ExternalSourceComparison } from "@/features/connections/external-source-comparison";
import { isAmbiguousMutationFailure } from "./mutation-recovery";
import { safeGitHubIssueURL } from "@/features/connections/github-url";
import { IssuePicker, type IssueSelection } from "@/features/connections/issue-picker";
import { useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";
import { ExternalLink, Feather, FileText, Github, Loader2, RefreshCw, Sparkles } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { ModelSelectField } from "./model-select-field";
import { getPlanningSessionDisplayTitleValue, hasPlaceholderTitle } from "./planning-session-title";
import { ThinkingEffortField } from "./thinking-effort-field";
import { PlannerThread } from "./planner-thread";
import { PlanResultModal } from "./planner-draft-panel";
import { ProviderModelCatalogNotice } from "./provider-model-catalog-state";
import { useProviderModelCatalogController } from "./use-provider-model-catalog";
import { usePlannerMessagePages } from "./use-planner-message-pages";
import { isPlannerOperationTerminal } from "./planner-operation-controller";
import { usePlanningSessionTitleController } from "./use-planning-session-title-controller";
import { usePlannerSessionController } from "./use-planner-session-controller";
import { useGlobalFormalize } from "./global-formalize-context";
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
  const externalSources = session?.external_sources ?? [];
  const updateTitle = useUpdatePlanningSessionTitle(sessionId);
  const updateModelSelection = useUpdatePlanningSessionModelSelection(sessionId);
  const recoverableAttachSource = useRecoverableMutation(
    "planning-attach-source",
    (intake: IssueSelection["intake"], key) => apiFetch(`/planning-sessions/${sessionId}/external-sources`, {
      method: "POST",
      headers: { "Idempotency-Key": key },
      body: JSON.stringify(intake),
    }),
    sessionId,
  );
  // Destructure stable handles up front. The hook object is recreated
  // each render, but `mutate`, `reset`, and `error` are stable across
  // renders (TanStack Query guarantee). Using these in effect deps
  // means cache patches that produce a new session reference don't
  // restart the autosave debounce timer.
  const {
    mutate: modelMutate,
    reset: modelReset,
    error: modelError,
		hasPendingRecovery: modelRecoveryPending,
		pendingVariables: pendingModelVariables,
		retryRecovery: retryModelRecovery,
	} = updateModelSelection;
	const providerModelCatalogController = useProviderModelCatalogController();
	const providerModelCatalog = providerModelCatalogController.state;
	const canonicalModel = session?.model_selection.model ?? "";
	const canonicalThinkingEffort = session?.model_selection.thinking_effort ?? "";
	const [modelSelectionDraft, setModelSelectionDraft] = useState(() => ({
		sessionId,
		canonicalModel,
		canonicalThinkingEffort,
		model: canonicalModel,
		thinkingEffort: canonicalThinkingEffort,
		editGeneration: 0,
		acknowledgedAutosave: undefined as
			| { model: string; thinkingEffort: string; editGeneration: number }
			| undefined,
	}));
	let currentModelSelection = modelSelectionDraft;
	if (
		modelSelectionDraft.sessionId !== sessionId
		|| modelSelectionDraft.canonicalModel !== canonicalModel
		|| modelSelectionDraft.canonicalThinkingEffort !== canonicalThinkingEffort
	) {
		const acknowledgedAutosave = modelSelectionDraft.acknowledgedAutosave;
		// Only an acknowledged older edit may yield to canonical state without
		// replacing a newer local edit that is still inside its debounce window.
		const preserveLaterEdit = modelSelectionDraft.sessionId === sessionId
			&& acknowledgedAutosave?.model === canonicalModel
			&& acknowledgedAutosave.thinkingEffort === canonicalThinkingEffort
			&& modelSelectionDraft.editGeneration > acknowledgedAutosave.editGeneration;
		currentModelSelection = preserveLaterEdit
			? {
				...modelSelectionDraft,
				canonicalModel,
				canonicalThinkingEffort,
				acknowledgedAutosave: undefined,
			}
			: {
				sessionId,
				canonicalModel,
				canonicalThinkingEffort,
				model: canonicalModel,
				thinkingEffort: canonicalThinkingEffort,
				editGeneration: 0,
				acknowledgedAutosave: undefined,
			};
		// Record the transition so returning to an earlier canonical value
		// cannot make a draft from that earlier generation current again.
		setModelSelectionDraft(currentModelSelection);
	}
	const model = currentModelSelection.model;
	const selectedModel = providerModelCatalog.items.find((item) => item.id === model);
	const thinkingEffort = model.trim()
		? normalizeThinkingEffortSelection(selectedModel, currentModelSelection.thinkingEffort, {
			preserveWhenModelMissing: true,
		})
		: currentModelSelection.thinkingEffort;
	const messagePages = usePlannerMessagePages(sessionId);
  const userMessageCount = messagePages.messages.filter((m) => m.role === "user").length;
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
      if (updateTitle.hasPendingRecovery) {
        try {
          await updateTitle.retryRecovery();
          setTitleSaveError(null);
        } catch (err) {
          setTitleSaveError({ message: err instanceof Error ? err.message : "Could not save title" });
          throw err;
        }
        return;
      }
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
    if (updateTitle.hasPendingRecovery) {
      try {
        await commitTitle(trimmed);
        exitEdit();
      } catch {
        // commitTitle keeps the exact recovery request and error visible.
      }
      return;
    }
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
  }, [session, titleInput, titleSaveError, updateTitle.hasPendingRecovery, commitTitle, exitEdit, abandonEdit]);

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

  const hasModelSelectionChanges =
    Boolean(session) &&
    (model !== (session?.model_selection.model ?? "") ||
      thinkingEffort !== (session?.model_selection.thinking_effort ?? ""));

  const hasUserMessages = userMessageCount > 0;
  const formalizePending = activeOperation?.kind === "formalize" && !isPlannerOperationTerminal(activeOperation.status);
  const hasPlan = Boolean(session?.current_draft_id);
  const showPlanConsumedForSession = useRef<string>("");
	const submitModelSelection = useCallback((
		submittedModel: string,
		submittedThinkingEffort: string,
		editGeneration: number,
	) => {
		if (modelRecoveryPending) return;
		const normalizedModel = submittedModel.trim();
		const normalizedThinkingEffort = submittedThinkingEffort.trim();
		modelMutate({
			modelSelection: normalizedModel
				? {
					model: normalizedModel,
					...(normalizedThinkingEffort ? { thinking_effort: normalizedThinkingEffort } : {}),
				}
				: undefined,
			key: newIdempotencyKey("planning-update-model"),
			editGeneration,
		}, {
			onSuccess: (result) => {
				// Superseded sliced mutations resolve with a symbol, not a response.
				if (typeof result !== "object" || result === null) return;
				setModelSelectionDraft((current) => (
					current.sessionId === sessionId && current.editGeneration > editGeneration
						? {
							...current,
							acknowledgedAutosave: {
								model: result.model_selection.model ?? "",
								thinkingEffort: result.model_selection.thinking_effort ?? "",
								editGeneration,
							},
						}
						: current
				));
			},
		});
	}, [modelMutate, modelRecoveryPending, sessionId]);
	const handleModelRecovery = () => {
		const editGeneration = pendingModelVariables?.editGeneration;
		void retryModelRecovery().then((result) => {
			if (typeof result !== "object" || result === null || editGeneration === undefined) return;
			setModelSelectionDraft((current) => (
				current.sessionId === sessionId && current.editGeneration > editGeneration
					? {
						...current,
						acknowledgedAutosave: {
							model: result.model_selection.model ?? "",
							thinkingEffort: result.model_selection.thinking_effort ?? "",
							editGeneration,
						},
					}
					: current
			));
		}).catch(() => {});
	};

  const attachSource = async ({ intake }: IssueSelection) => {
    await recoverableAttachSource.mutateAsync(intake);
    await sessionQuery.refetch();
  };

  // Auto-open plan modal when arriving via ?show_plan=true (e.g. from notification "View Plan" link)
  useEffect(() => {
    if (!showPlanParam) {
      showPlanConsumedForSession.current = "";
      return;
    }
    if (hasPlan && showPlanConsumedForSession.current !== sessionId) {
      showPlanConsumedForSession.current = sessionId;
      queueMicrotask(() => {
        if (showPlanConsumedForSession.current === sessionId) {
          setShowPlanModal(true);
        }
      });
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
    if (!hasModelSelectionChanges || modelRecoveryPending) return;
    const timer = setTimeout(() => {
		submitModelSelection(model, thinkingEffort, currentModelSelection.editGeneration);
    }, 500);
    return () => clearTimeout(timer);
  }, [model, thinkingEffort, hasModelSelectionChanges, modelRecoveryPending, currentModelSelection.editGeneration, submitModelSelection]);

  // When the user reverts the dropdown back to canonical, any prior
  // failure is now meaningless — the chip's Retry would re-send the
  // canonical value and a stale error would survive subsequent edits
  // until the next attempt's onMutate fired. Calling reset() clears
  // the mutation state so future dirty edits start clean.
  useEffect(() => {
    if (!hasModelSelectionChanges && modelError && !modelRecoveryPending) {
      modelReset();
    }
  }, [hasModelSelectionChanges, modelError, modelRecoveryPending, modelReset]);

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
                disabled={updateTitle.hasPendingRecovery}
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
                {!updateTitle.hasPendingRecovery ? (
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
                ) : null}
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
                  disabled={providerModelCatalog.disableSelection || modelRecoveryPending}
                options={providerModelCatalog.items}
                triggerPlaceholder={providerModelCatalog.triggerPlaceholder}
                emptyStateLabel={providerModelCatalog.emptyStateLabel}
                onChange={(next) => {
                  const nextModel = providerModelCatalog.items.find((item) => item.id === next);
                  setModelSelectionDraft({
                    ...currentModelSelection,
                    model: next,
                    thinkingEffort: normalizeThinkingEffortSelection(nextModel, "", {
                      preserveWhenModelMissing: true,
                    }),
					editGeneration: currentModelSelection.editGeneration + 1,
                  });
                }}
              />
            </div>
            <div className="w-[240px]">
              <ThinkingEffortField
                id="planning-thinking-effort"
                label="Thinking Effort"
                value={thinkingEffort}
                model={selectedModel}
                disabled={providerModelCatalog.disableSelection || modelRecoveryPending}
                onChange={(next) => {
					setModelSelectionDraft({
						...currentModelSelection,
						thinkingEffort: next,
						editGeneration: currentModelSelection.editGeneration + 1,
					});
                }}
              />
            </div>
          </div>
          {(hasModelSelectionChanges || modelRecoveryPending) && modelError ? (
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
                onClick={modelRecoveryPending ? handleModelRecovery : () => {
				  submitModelSelection(model, thinkingEffort, currentModelSelection.editGeneration);
                }}
              >
                Retry
              </button>
            </div>
          ) : null}
        </div>
      </div>

      <aside aria-label="Linked GitHub sources" className="mx-4 mt-3 max-h-[min(42vh,28rem)] shrink-0 overflow-y-auto rounded-lg border border-gold/20 bg-gold/[0.03] p-2.5">
          <div className="mb-2 flex flex-col gap-2 px-1 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex items-center gap-2">
              <p className="text-[10px] font-medium uppercase tracking-[0.18em] text-foreground-subtle">Planning sources</p>
              <span className="rounded-full border border-gold/20 bg-gold/5 px-2 py-0.5 text-[10px] text-gold">{externalSources.length}</span>
            </div>
            <IssuePicker onSelect={attachSource} actionLabel="Add source" triggerLabel={externalSources.length > 0 ? "Add source" : "Add GitHub issue"} compact />
          </div>
          {externalSources.length > 0 ? (
            <div className="grid gap-3 lg:grid-cols-2">
              {externalSources.map((source) => <PlanningExternalSourceCard key={source.link_id} source={source} onChanged={() => sessionQuery.refetch()} />)}
            </div>
          ) : <p className="px-1 pb-1 text-xs text-foreground-muted">Add a GitHub issue as immutable context for future planner operations.</p>}
      </aside>

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
        planningLineage={draftQuery.data ? {
          planning_session_id: sessionId,
          planning_draft_id: draftQuery.data.planning_draft_id,
        } : undefined}
        operation={activeOperation?.kind === "formalize" ? activeOperation : null}
        onClose={() => setShowPlanModal(false)}
      />
    </div>
  );
}

type PlanningExternalSource = PlanningSessionDetail["external_sources"][number];
type ExternalRefreshResult = components["schemas"]["ExternalRefreshResult"];

function PlanningExternalSourceCard({
  source,
  onChanged,
}: {
  source: PlanningExternalSource;
  onChanged: () => Promise<unknown>;
}) {
  const [busy, setBusy] = useState<"refresh" | "adopt" | "">("");
  const [error, setError] = useState("");
  const [refreshResult, setRefreshResult] = useState<ExternalRefreshResult>();
  const refreshKeyRef = useRef<string | undefined>(undefined);
  const adoptKeyRef = useRef<string | undefined>(undefined);
  const activeSnapshotID = source.active_snapshot_id ?? source.used_snapshot_id;
  const latestSnapshotID = refreshResult?.link?.latest_snapshot_id ?? source.latest_snapshot_id;
  const sourceStatus = refreshResult?.outcome ?? source.status;
  const updateAvailable = latestSnapshotID !== activeSnapshotID;
  const failureGuidance = externalSourceFailureGuidance(sourceStatus);
  const sourceURL = safeGitHubIssueURL(source.canonical_url, source.repository, source.issue_number);

  const refresh = async () => {
    setBusy("refresh");
    setError("");
    const key = refreshKeyRef.current ?? newIdempotencyKey("external-refresh");
    refreshKeyRef.current = key;
    try {
      const result = await apiFetch<ExternalRefreshResult>(`/external-source-links/${source.link_id}/refreshes`, {
        method: "POST",
        headers: { "Idempotency-Key": key },
      });
      refreshKeyRef.current = undefined;
      setRefreshResult(result);
      await onChanged();
    } catch (cause) {
      if (!isAmbiguousMutationFailure(cause)) refreshKeyRef.current = undefined;
      setError(cause instanceof Error ? cause.message : "Could not refresh GitHub source");
    } finally {
      setBusy("");
    }
  };

  const adopt = async () => {
    setBusy("adopt");
    setError("");
    const key = adoptKeyRef.current ?? newIdempotencyKey("external-adopt");
    adoptKeyRef.current = key;
    try {
      await apiFetch(`/external-source-links/${source.link_id}/adoptions`, {
        method: "POST",
        headers: { "Idempotency-Key": key },
      });
      adoptKeyRef.current = undefined;
      setRefreshResult(undefined);
      await onChanged();
    } catch (cause) {
      if (!isAmbiguousMutationFailure(cause)) adoptKeyRef.current = undefined;
      setError(cause instanceof Error ? cause.message : "Could not adopt the latest GitHub source");
    } finally {
      setBusy("");
    }
  };

  return (
    <section aria-label={`Linked GitHub source ${source.repository}#${source.issue_number}`} className="min-w-0 space-y-3 rounded-lg border border-gold/25 bg-gold/5 p-3 text-xs">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="font-medium text-foreground"><Github className="mr-1 inline h-3.5 w-3.5" />{sourceURL ? <a href={sourceURL} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-gold underline">{source.repository}#{source.issue_number}<ExternalLink className="h-3 w-3" /></a> : <span>{source.repository}#{source.issue_number}</span>}</p>
          <p className="mt-1 text-foreground-muted">{externalSourceStatusLabel(sourceStatus, updateAvailable)}{updateAvailable ? ". Adopt the update to use it in future planner operations." : "."}</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <button type="button" aria-label={`Refresh ${source.repository}#${source.issue_number}`} disabled={busy !== ""} className="rounded border border-border px-2 py-1 disabled:opacity-50" onClick={() => void refresh()}>
            {busy === "refresh" ? <Loader2 className="mr-1 inline h-3 w-3 animate-spin" /> : <RefreshCw className="mr-1 inline h-3 w-3" />}Refresh
          </button>
          {updateAvailable ? (
            <button type="button" aria-label={`Adopt latest ${source.repository}#${source.issue_number}`} disabled={busy !== ""} className="rounded bg-gold px-2 py-1 font-medium text-black disabled:opacity-50" onClick={() => void adopt()}>
              {busy === "adopt" ? "Adopting…" : "Adopt latest"}
            </button>
          ) : null}
        </div>
      </div>
      {failureGuidance && !refreshResult ? <p role="status" className="text-status-failed">{failureGuidance}</p> : null}
      {updateAvailable ? <ExternalSourceComparison baselineId={activeSnapshotID} candidateId={latestSnapshotID} /> : null}
      {refreshResult ? (
        <div aria-live="polite" className="space-y-1 border-t border-gold/15 pt-2 text-foreground-muted">
          <p className="font-medium text-foreground">{refreshResult.message}</p>
          {refreshResult.retry_at ? <p>Retry after {new Date(refreshResult.retry_at).toLocaleString()}.</p> : null}
        </div>
      ) : null}
      {error ? <p role="alert" className="text-status-failed">{error}</p> : null}
      <details className="text-foreground-subtle"><summary className="cursor-pointer text-gold">Source details</summary><dl className="mt-1 space-y-1 font-mono text-[11px]"><div><dt className="inline">Used snapshot: </dt><dd className="inline break-all">{source.used_snapshot_id}</dd></div><div><dt className="inline">Active snapshot: </dt><dd className="inline break-all">{activeSnapshotID}</dd></div><div><dt className="inline">Latest snapshot: </dt><dd className="inline break-all">{latestSnapshotID}</dd></div></dl></details>
    </section>
  );
}
