import {
  newIdempotencyKey,
  usePlanningOperation,
  usePlanningSession,
  usePlanningSessionDraft,
  useUpdatePlanningSessionModelSelection,
  useUpdatePlanningSessionTitle,
} from "@/api/hooks";
import { apiFetch } from "@/api/client";
import { type IssueSelection } from "@/features/connections/issue-picker";
import { PlanningSourcesPanel } from "./planning-sources-panel";
import { Link, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import { usePlanningMutationJournal } from "./use-planning-mutation-journal";
import { ChevronDown } from "lucide-react";
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
import { usePlannerSessionUI } from "./use-planner-workspace-state";
import { PlanningContextPanel } from "./planning-context-panel";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { Button } from "@/components/ui/button";
import { buttonVariants } from "@/components/ui/button-variants";
import { useThemeCopy } from "@/theme";
import { normalizeThinkingEffortSelection } from "./provider-model-selection";

export function PlanningSessionPage() {
  const { sessionId } = useParams({ from: "/work-orders/planning/$sessionId" });
  const { show_plan: showPlanParam } = useSearch({ from: "/work-orders/planning/$sessionId" });
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const sessionQuery = usePlanningSession(sessionId);
  const session = sessionQuery.data;
  const workName = useThemeCopy("workOrder.singular");
  const uiScope = { workspace: session?.project_root ?? "__pending__", sessionId };
  const { store: uiStore, state: sessionUI } = usePlannerSessionUI(uiScope);
  const tab = sessionUI.selectedTab ?? "chat";
  const setTab = (selectedTab: "chat" | "plan") => uiStore.updateSession(uiScope, (state) => ({ ...state, selectedTab }));
  const externalSources = session?.external_sources ?? [];
  const updateTitle = useUpdatePlanningSessionTitle(sessionId);
  const updateModelSelection = useUpdatePlanningSessionModelSelection(sessionId);
  const recoverableAttachSource = usePlanningMutationJournal(
    "planning-attach-source",
    (intake: IssueSelection["intake"], key) => apiFetch(`/planning-sessions/${sessionId}/external-sources`, {
      method: "POST",
      headers: { "Idempotency-Key": key },
      body: JSON.stringify(intake),
    }),
    sessionId,
    "planning-source-attach",
  );
  const [attachmentBusy, setAttachmentBusy] = useState(false);
  const [attachmentError, setAttachmentError] = useState("");
  const recoverAttachment = async () => {
    setAttachmentBusy(true);
    setAttachmentError("");
    try { await recoverableAttachSource.retry(); await sessionQuery.refetch(); }
    catch (error) { setAttachmentError(error instanceof Error ? error.message : "The source attachment remains unresolved."); }
    finally { setAttachmentBusy(false); }
  };
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
  const { activeOperation, activeOperationId, connection, operationQueryState, recoveryKind, recoveryBlocked, recovery, stop, retryReplyRecovery, setActiveOperationId, submitReply, startFormalize, createOperation, regenerate, activateVariant } =
    usePlannerSessionController(session);
  const { entries: formalizeEntries, trackFormalize } = useGlobalFormalize();
  const globalFormalize = formalizeEntries.find((entry) => entry.sessionId === sessionId);
	const draftQuery = usePlanningSessionDraft(showPlanModal || tab === "plan" || session?.current_draft_id ? sessionId : undefined);
	const [titleStreamRunID, setTitleStreamRunID] = useState<string | undefined>(session?.current_title_run_id);
	if (session?.current_title_run_id && session.current_title_run_id !== titleStreamRunID) {
		setTitleStreamRunID(session.current_title_run_id);
	}
	const titleOperationQuery = usePlanningOperation(titleStreamRunID);
	const { operation: titleStreamOperation } = usePlannerOperation(titleOperationQuery.data ?? null);
	if (titleStreamRunID && !session?.current_title_run_id && titleStreamOperation && isPlannerOperationTerminal(titleStreamOperation.status)) {
		setTitleStreamRunID(undefined);
	}
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
    if (recoverableAttachSource.getPendingRequest()) throw new Error("Recover the previous source attachment before selecting another issue.");
    setAttachmentBusy(true);
    setAttachmentError("");
    try { await recoverableAttachSource.mutateAsync(intake); await sessionQuery.refetch(); }
    catch (error) { setAttachmentError(error instanceof Error ? error.message : "The source attachment remains unresolved."); throw error; }
    finally { setAttachmentBusy(false); }
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
        to: "/work-orders/planning/$sessionId",
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
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-accent border-t-transparent" />
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
            className="ui-button ui-button--outline rounded-md border border-border bg-background px-3 py-1.5 text-sm font-medium text-foreground transition-colors hover:bg-surface-raised/60"
          >
            Try Again
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

  const modelControls = <details className="mt-3 text-xs text-foreground-muted" open={modelError || modelRecoveryPending ? true : undefined}>
    <summary aria-label="Planner model settings" className="flex w-fit cursor-pointer list-none items-center gap-2 rounded-md py-1.5 pr-2 hover:text-foreground">{selectedModel?.model_name ?? (model || "Choose Planner Model")}{thinkingEffort ? ` · ${thinkingEffort}` : ""}<ChevronDown className="h-3 w-3" /></summary>
        <div className="space-y-3 border-t border-border/60 pt-3">
          <div className="flex flex-wrap items-end gap-3">
            <div className="w-full">
              <ProviderModelCatalogNotice
                state={providerModelCatalog}
				onRetry={providerModelCatalogController.retry}
				retryPending={providerModelCatalogController.retryPending}
              />
            </div>
            <div className="min-w-0 max-w-xs flex-1 basis-52">
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
            <div className="min-w-0 max-w-xs flex-1 basis-52">
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
          {(hasModelSelectionChanges && modelError) || modelRecoveryPending ? (
            <div
              role="alert"
              className="flex w-full items-center gap-2 rounded-md border border-status-failed/30 bg-status-failed/10 px-2 py-1 text-xs text-status-failed"
            >
              <span className="truncate" title={modelError?.message}>
                {modelError ? `Couldn’t save model selection: ${modelError.message}` : "A previous model save is unconfirmed."}
              </span>
              {modelRecoveryPending && <details className="max-w-xs"><summary className="cursor-pointer">Request Details</summary><p className="break-all">{pendingModelVariables?.modelSelection?.model} {pendingModelVariables?.modelSelection?.thinking_effort}</p><code className="break-all">{updateModelSelection.pendingRequestKey}</code></details>}
              <button
                type="button"
                className="ui-button ui-button--outline ml-auto rounded border border-status-failed/40 bg-status-failed/15 px-2 py-0.5 font-medium hover:bg-status-failed/25"
                onClick={modelRecoveryPending ? handleModelRecovery : () => {
				  submitModelSelection(model, thinkingEffort, currentModelSelection.editGeneration);
                }}
              >
                Retry
              </button>
            </div>
          ) : null}
        </div>
  </details>;

  return (
    <div
      data-testid="planning-session-content"
      className="flex h-full min-h-0 flex-col bg-background"
    >
      <div className="relative z-20 shrink-0 space-y-4 border-b border-border px-4 pt-5 sm:px-8">
        <div className="flex items-center justify-between gap-3">
          <div className="flex min-w-0 flex-1 items-center gap-2">
            {isEditingTitle ? (
              <input data-ui-field=""
                autoFocus
                type="text"
                value={titleInput}
                disabled={updateTitle.hasPendingRecovery}
                onChange={(e) => setTitleInput(e.target.value)}
                // Don't blur-commit while a save error is showing — the
                // user is mid-recovery and a re-blur would re-fire the
                // commit before they got to retry/discard.
                onBlur={
                  titleSaveError
                    ? undefined
                    : () => {
                        void handleCommit();
                      }
                }
                onKeyDown={(e) => {
                  if (e.key === "Enter") void handleCommit();
                }}
                placeholder="Session title..."
                className={`flex-1 border-b ${titleSaveError ? "border-status-failed" : "border-accent"} bg-transparent text-sm font-semibold text-foreground placeholder:text-foreground-subtle/50 focus:outline-none`}
              />
            ) : (
              <button
                type="button"
                className="min-w-0 truncate text-left"
                onClick={startEditing}
                aria-label="Edit session title"
              >
                {isRevealingTitle ? (
                  <span className="flex items-center gap-[0.25em] text-sm font-semibold text-foreground">
                    {displayTitle
                      .split(/\s+/)
                      .filter(Boolean)
                      .map((word, i) => (
                        <span
                          key={i}
                          className="inline-block opacity-0"
                          style={{
                            animation:
                              "title-word-reveal 250ms ease-out forwards",
                            animationDelay: `${i * 100}ms`,
                          }}
                        >
                          {word}
                        </span>
                      ))}
                  </span>
                ) : (
                  <span
                    className={`truncate text-sm font-semibold ${isPlaceholderHeaderTitle ? "italic text-foreground-subtle" : "text-foreground"}`}
                  >
                    {visibleDisplayTitle}
                  </span>
                )}
              </button>
            )}
            {titleSaveError || updateTitle.hasPendingRecovery ? (
              <div
                role="alert"
                className="ml-2 flex shrink-0 items-center gap-2 rounded-md border border-status-failed/30 bg-status-failed/10 px-2 py-1 text-xs text-status-failed"
              >
                <span className="truncate" title={titleSaveError?.message}>
                  {titleSaveError?.message ??
                    "A previous title save is unconfirmed."}
                </span>
                <button
                  type="button"
                  className="ui-button ui-button--outline rounded border border-status-failed/40 bg-status-failed/15 px-2 py-0.5 font-medium hover:bg-status-failed/25"
                  onClick={() => {
                    void handleCommit();
                  }}
                >
                  Retry
                </button>
                {updateTitle.hasPendingRecovery && (
                  <details className="max-w-xs">
                    <summary className="cursor-pointer">
                      Request Details
                    </summary>
                    <p className="break-all">
                      {updateTitle.pendingVariables?.title}
                    </p>
                    <code className="break-all">
                      {updateTitle.pendingRequestKey}
                    </code>
                  </details>
                )}
                {!updateTitle.hasPendingRecovery ? (
                  <button
                    type="button"
                    className="ui-button ui-button--ghost rounded border border-border/40 bg-background/40 px-2 py-0.5 font-medium text-foreground-subtle hover:bg-surface-raised/40"
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
            <button
              type="button"
              onClick={() => void handleFormalizePlan().catch(() => {})}
              disabled={
                formalizePending ||
                !hasUserMessages ||
                session.conversation_state !== "ready" ||
                Boolean(recovery.pendingKind) ||
                recoveryBlocked
              }
              className="ui-button ui-button--default flex items-center gap-1.5 rounded-md border border-accent/50 bg-accent/10 px-3 py-1.5 text-xs font-semibold text-accent transition-all hover:bg-accent/20 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {formalizePending ? (
                <div className="h-3 w-3 animate-spin rounded-full border-2 border-accent border-t-transparent" />
              ) : null}
              {formalizePending ? "Generating Plan..." : "Generate Plan"}
            </button>
          </div>
        </div>

        <div className="flex flex-wrap items-start justify-between gap-x-6 gap-y-3">
          <div
            data-ui-tabs=""
            role="tablist"
            aria-label="Planning workspace"
            className="flex gap-6"
          >
            {(["chat", "plan"] as const).map((value, index, tabs) => (
              <button
                key={value}
                data-ui-tab=""
                role="tab"
                id={`planning-tab-${value}`}
                aria-controls={`planning-panel-${value}`}
                aria-selected={tab === value}
                tabIndex={tab === value ? 0 : -1}
                onClick={() => setTab(value)}
                onKeyDown={(event) => {
                  const offset =
                    event.key === "ArrowRight"
                      ? 1
                      : event.key === "ArrowLeft"
                        ? -1
                        : 0;
                  if (!offset && event.key !== "Home" && event.key !== "End")
                    return;
                  event.preventDefault();
                  const next =
                    event.key === "Home"
                      ? tabs[0]
                      : event.key === "End"
                        ? tabs[tabs.length - 1]
                        : tabs[(index + offset + tabs.length) % tabs.length];
                  setTab(next);
                  document.getElementById(`planning-tab-${next}`)?.focus();
                }}
                className={`relative border-b-2 px-1 pb-3 text-sm font-medium transition-colors ${tab === value ? "border-accent text-foreground" : "border-transparent text-foreground-muted hover:text-foreground"}`}
              >
                {value === "chat" ? "Chat" : "Plan"}
                {value === "plan" && (hasPlan || formalizePending) ? (
                  <span
                    className="ml-2 inline-block h-1.5 w-1.5 rounded-full bg-accent"
                    aria-label={
                      formalizePending ? "Generating" : "Plan available"
                    }
                  />
                ) : null}
              </button>
            ))}
          </div>
          <div className="flex min-w-0 flex-wrap items-start gap-2 pb-3">
            <PlanningSourcesPanel
              key={`sources:${sessionId}`}
              attachmentPending={recoverableAttachSource.hasPendingRecovery}
              workspace={session.project_root}
              sessionId={sessionId}
              sources={externalSources}
              onAttach={attachSource}
              onChanged={() => sessionQuery.refetch()}
            />
            <PlanningContextPanel
              key={`context:${sessionId}`}
              session={session}
              onChanged={() => sessionQuery.refetch()}
            />
          </div>
        </div>
      </div>

      {/* Session-load errors are handled by the early-return shell above. */}
      {createOperation.error ? (
        <div className="mx-4 mt-2 rounded-lg border border-status-failed/30 bg-status-failed/10 p-2.5 text-xs text-status-failed">
          {createOperation.error.message}
        </div>
      ) : null}

      <div
        role="tabpanel"
        id="planning-panel-chat"
        aria-labelledby="planning-tab-chat"
        hidden={tab !== "chat"}
        className={tab === "chat" ? "flex min-h-0 flex-1 flex-col" : "hidden"}
      >
        <PlannerThread
          composerControls={modelControls}
          session={session}
          operation={activeOperation}
          connection={connection}
          operationQueryState={operationQueryState}
          replyRecoveryPending={recoveryKind === "reply"}
          recoveryBlocked={recoveryBlocked}
          recovery={recovery}
          stop={stop}
          onRetryReplyRecovery={retryReplyRecovery}
          onSubmitReply={submitReply}
          onRegenerate={({ messageId, parentMessageId }) =>
            regenerate({ messageId, parentMessageId })
          }
          onActivateVariant={({ messageId }) => activateVariant({ messageId })}
        />
      </div>
      {tab === "plan" && (
        <section
          role="tabpanel"
          id="planning-panel-plan"
          aria-labelledby="planning-tab-plan"
          className="min-h-0 flex-1 overflow-y-auto px-4 py-8 sm:px-8"
        >
          <div className="mx-auto max-w-3xl space-y-6">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div>
                <h2 className="font-display text-3xl">
                  {formalizePending
                    ? "New Plan in Progress"
                    : draftQuery.data?.work_order_title || "Your Plan"}
                </h2>
                <p className="mt-2 text-sm text-foreground-muted">
                  {formalizePending
                    ? "The plan is taking shape. You can switch sessions while it runs."
                    : "Review the settled plan before moving into execution."}
                </p>
              </div>
              {draftQuery.data && !formalizePending && (
                <Link
                  className={buttonVariants()}
                  to="/work-orders/new"
                  search={{
                    planning_session_id: sessionId,
                    planning_draft_id: draftQuery.data.planning_draft_id,
                  }}
                >
                  Review &amp; Issue {workName}
                </Link>
              )}
            </div>
            {activeOperation?.kind === "formalize" && activeOperation.error ? (
              <p role="alert" className="text-sm text-status-failed">
                {activeOperation.error}
              </p>
            ) : null}
            {formalizePending && activeOperation?.formalizeText ? (
              <MarkdownBlock content={activeOperation.formalizeText} />
            ) : !formalizePending && draftQuery.data ? (
              <MarkdownBlock content={draftQuery.data.work_order_text} />
            ) : (
              <div className="rounded-2xl border border-dashed border-border p-8 text-sm text-foreground-muted">
                {draftQuery.isLoading || (draftQuery.isFetching && !draftQuery.data)
                  ? "Loading the saved plan..."
                  : formalizePending
                    ? "Generating your plan..."
                    : "When the direction feels right, generate a plan from this conversation."}
              </div>
            )}
            {draftQuery.error && (
              <div role="alert" className="text-sm text-status-failed">
                The saved plan could not be loaded.
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => void draftQuery.refetch()}
                >
                  Retry
                </Button>
              </div>
            )}
            {stop.available && formalizePending && (
              <Button
                variant="outline"
                disabled={stop.pending}
                onClick={() => void stop.onStop().catch(() => {})}
              >
                Stop Generation
              </Button>
            )}
            {formalizePending && draftQuery.data && (
              <details className="rounded-xl border border-border p-4">
                <summary className="cursor-pointer text-sm font-medium">
                  Previous Saved Plan
                </summary>
                <div className="mt-4 space-y-4">
                  <h3 className="font-display text-2xl">
                    {draftQuery.data.work_order_title}
                  </h3>
                  <MarkdownBlock content={draftQuery.data.work_order_text} />
                  <Link
                    className={buttonVariants({ variant: "outline" })}
                    to="/work-orders/new"
                    search={{
                      planning_session_id: sessionId,
                      planning_draft_id: draftQuery.data.planning_draft_id,
                    }}
                  >
                    Review &amp; Issue Saved {workName}
                  </Link>
                </div>
              </details>
            )}
          </div>
        </section>
      )}
      {recoverableAttachSource.pendingRequest && !attachmentBusy && (
        <div
          className="mx-4 mb-3 space-y-2 rounded-lg border border-status-warning/30 bg-status-warning/5 p-3 text-xs sm:mx-8"
          role="status"
        >
          <p>
            The source attachment for issue #
            {recoverableAttachSource.pendingRequest.issue_number} needs
            recovery. Resolve this request before selecting another issue.
          </p>
          <details>
            <summary className="cursor-pointer font-medium">
              Attachment Request Details
            </summary>
            <dl className="mt-2 space-y-1">
              <dt>Repository ID</dt>
              <dd className="break-all">
                {recoverableAttachSource.pendingRequest.repository_id}
              </dd>
              <dt>Request ID</dt>
              <dd className="break-all font-mono">
                {recoverableAttachSource.pendingKey}
              </dd>
            </dl>
          </details>
          {recoverableAttachSource.recoveryBlocked && (
            <p>
              This request belongs to an earlier browser session. Its outcome
              remains unresolved; it will not be replayed.
            </p>
          )}
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() => void recoverAttachment()}
          >
            Recover Source Attachment
          </Button>
          {attachmentError && (
            <p role="alert" className="text-status-failed">
              {attachmentError}
            </p>
          )}
        </div>
      )}
      <PlanResultModal
        open={showPlanModal}
        draft={draftQuery.data ?? null}
        planningLineage={
          draftQuery.data
            ? {
                planning_session_id: sessionId,
                planning_draft_id: draftQuery.data.planning_draft_id,
              }
            : undefined
        }
        operation={
          activeOperation?.kind === "formalize" ? activeOperation : null
        }
        onClose={() => setShowPlanModal(false)}
      />
    </div>
  );
}
