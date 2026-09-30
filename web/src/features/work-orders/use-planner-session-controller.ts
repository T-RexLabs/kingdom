import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect } from "react";
import { type AcpRun, type CreateAcpRunRequest, useActivateVariant, useCreatePlanningOperation, usePlanningOperation, type PlanningSessionDetail } from "@/api/hooks";
import { apiFetch, currentAuthBinding, currentAuthGeneration } from "@/api/client";
import { newIdempotencyKey } from "@/lib/idempotency";
import { shouldRetainMutationRequest } from "@/lib/use-recoverable-mutation";
import { isPlannerOperationTerminal, type AnswerPayload, type PlannerOperationState } from "./planner-operation-controller";
import { usePlannerOperation } from "./use-planner-operation";
import { applyPlanningSessionFields } from "./planner-session-cache";
import { plannerKeys } from "./planner-query-keys";
import type { components } from "@/api/schema";
import { plannerScope, type PlannerDraftSubmission, type PlannerPendingRequest } from "./planner-workspace-state";
import { usePlanningMutationJournal } from "./use-planning-mutation-journal";
import { usePlannerSessionUI } from "./use-planner-workspace-state";

type ReplySubmission = { text: string; answer?: AnswerPayload; parentMessageId?: string; draft?: PlannerDraftSubmission };
type CreateKind = "reply" | "regenerate" | "formalize";

/** Route-independent intent owns requests; Query and ACP own canonical results. */
export function usePlannerSessionController(session: PlanningSessionDetail | undefined) {
  const queryClient = useQueryClient();
  const scope = plannerScope(session ?? { planning_session_id: "", project_root: "" });
  const { workspace, sessionId } = scope;
  const { store, state } = usePlannerSessionUI(scope);
  const createOperationMutation = useCreatePlanningOperation();
  const activateVariantMutation = useActivateVariant(sessionId);
  const branchRecovery = usePlanningMutationJournal("planning-branch", (messageId: string, key) => activateVariantMutation.mutateAsync({ value: messageId, key }), sessionId);
  const operationId = state.trackedOperationId ?? session?.current_run_id;
  const activeOperationQuery = usePlanningOperation(operationId);
  const { operation: streamed, connection } = usePlannerOperation(activeOperationQuery.data ?? null);
  const canonicalOperation = streamed?.operationId === operationId ? streamed : null;
  const activeOperation = state.optimisticOperation ?? canonicalOperation;
  const activeOperationId = operationId && !isPlannerOperationTerminal(activeOperation?.status ?? "pending") ? operationId : undefined;
  const recoveryKind = state.request?.phase === "recovery" ? state.request.kind : null;
  const recoveryBlocked = Boolean(state.request && !store.canRetry(state.request)) || branchRecovery.recoveryBlocked;
  const setActiveOperationId = useCallback((next: string | undefined) => {
    store.updateSession({ workspace, sessionId }, (current) => current.trackedOperationId === next ? current : { ...current, trackedOperationId: next });
  }, [store, workspace, sessionId]);

  useEffect(() => {
    if (!session || !canonicalOperation) return;
    const entry = store.getOperations().find((item) => item.operationId === canonicalOperation.operationId) ?? {
      workspace, sessionId, operationId: canonicalOperation.operationId, kind: canonicalOperation.kind, sessionTitle: session.title,
    };
    if (!isPlannerOperationTerminal(canonicalOperation.status) || (canonicalOperation.status === "done" && !canonicalOperation.activityReconciled)) store.trackOperation(entry);
    store.observeOperation(entry, canonicalOperation);
    if (canonicalOperation.status === "error" || canonicalOperation.status === "cancelled") setActiveOperationId(canonicalOperation.operationId);
  }, [canonicalOperation, session, store, workspace, sessionId, setActiveOperationId]);

  const perform = async (kind: CreateKind, body: CreateAcpRunRequest, optimistic: Partial<PlannerOperationState>, draftBranch?: string, draft?: PlannerDraftSubmission) => {
    if (!session) throw new Error("Planning session is required");
    if (branchRecovery.getPendingRequest()) throw new Error("Resolve the previous branch change before starting another planner operation.");
    const current = store.getSession(scope);
    if (current.request?.phase === "submitting") throw new Error("A planner request is already being submitted");
    if (current.request && current.request.kind !== kind) throw new Error(`Retry the pending ${current.request.kind} request before starting another planner operation`);
    if (current.request && !store.canRetry(current.request)) throw new Error("The previous request must be verified before it can be retried in this browser session");
    const attempted: PlannerPendingRequest = current.request ?? { kind, body, key: newIdempotencyKey(`planning-${kind}`), authGeneration: currentAuthGeneration(), authBinding: currentAuthBinding(), restored: false, phase: "submitting", draftBranch, draft, parentMessageId: optimistic.parentMessageId };
    const requestBody = attempted.body as CreateAcpRunRequest;
    const sequence = current.sequence + 1;
    const liveSession = queryClient.getQueryData<PlanningSessionDetail>(plannerKeys.detail(sessionId));
    const previousConversationState = liveSession?.conversation_state;
    const now = new Date().toISOString();
    const pending: PlannerOperationState = {
      kind, operationId: `pending-${kind}-${sequence}`, status: "pending", lastEventSeq: 0,
      pendingReply: kind === "reply" && requestBody.reply ? { contentMarkdown: requestBody.reply.content_markdown, answer: requestBody.reply.answer_payload } : null,
      replyText: "", formalizeText: "", error: null, activityReconciled: false, events: [], activityItems: [],
      ...(kind === "reply" ? { userMessageId: `pending-user-${sequence}`, userCreatedAt: now } : {}),
      ...(kind !== "formalize" ? { assistantMessageId: `pending-assistant-${sequence}`, assistantCreatedAt: now } : {}),
      ...optimistic,
      parentMessageId: attempted.parentMessageId,
    };
    store.journalRequest(scope, { ...attempted, phase: "submitting" }, (value) => ({ ...value, sequence, optimisticOperation: pending }));
    if (kind !== "formalize") applyPlanningSessionFields(queryClient, sessionId, { conversation_state: "processing" });
    try {
      const created = await createOperationMutation.mutateAsync({ body: requestBody, key: attempted.key });
      store.updateSession(scope, (value) => value.sequence === sequence ? { ...value, request: undefined, optimisticOperation: null, trackedOperationId: created.run_id } : value);
      store.trackOperation({ ...scope, operationId: created.run_id, kind, sessionTitle: session.title, draftBranch: attempted.draftBranch });
      if (attempted.draft) store.clearSubmittedDraft(scope, attempted.draft.branch, attempted.draft.generation, attempted.draft.kind === "edit");
      applyPlanningSessionFields(queryClient, sessionId, { conversation_state: "processing", current_run_id: created.run_id });
      void queryClient.invalidateQueries({ queryKey: plannerKeys.messages(sessionId) });
      return created;
    } catch (error) {
      store.updateSession(scope, (value) => value.sequence === sequence ? {
        ...value, optimisticOperation: current.optimisticOperation,
        request: shouldRetainMutationRequest(error, Boolean(current.request)) ? { ...attempted, phase: "recovery" } : undefined,
        error: error instanceof Error ? error.message : "Could not submit planner request",
      } : value);
      // Revert only our processing marker, never a whole stale session or transcript.
      const live = queryClient.getQueryData<PlanningSessionDetail>(plannerKeys.detail(sessionId));
      if (previousConversationState && kind !== "formalize" && live?.conversation_state === "processing" && live.current_run_id === liveSession?.current_run_id) {
        applyPlanningSessionFields(queryClient, sessionId, { conversation_state: previousConversationState });
      }
      throw error;
    }
  };

  const stopOperation = async () => {
    if (!session || !activeOperationId) return;
    const current = store.getSession(scope);
    if (current.request?.phase === "submitting") return;
    if (current.request && current.request.kind !== "stop") throw new Error("Resolve the previous planner request before stopping this operation");
    if (current.request && !store.canRetry(current.request)) throw new Error("The previous stop request must be verified before retrying");
    const attempted: PlannerPendingRequest = current.request ?? { kind: "stop", body: { runId: activeOperationId }, key: newIdempotencyKey("planning-stop"), authGeneration: currentAuthGeneration(), authBinding: currentAuthBinding(), restored: false, phase: "submitting" };
    const runId = (attempted.body as { runId: string }).runId;
    store.journalRequest(scope, { ...attempted, phase: "submitting" });
    try {
      const run = await apiFetch<AcpRun>(`/acp/runs/${runId}/cancel`, { method: "POST", headers: { "Idempotency-Key": attempted.key } });
      queryClient.setQueryData(plannerKeys.operation(runId), run);
      store.updateSession(scope, (value) => ({ ...value, request: undefined }));
      void queryClient.invalidateQueries({ queryKey: plannerKeys.detail(sessionId) });
    } catch (error) {
      store.updateSession(scope, (value) => ({ ...value, request: shouldRetainMutationRequest(error, Boolean(current.request)) ? { ...attempted, phase: "recovery" } : undefined, error: error instanceof Error ? error.message : "Could not stop planner operation" }));
      throw error;
    }
  };

  const checkRequestOutcome = async () => {
    const current = store.getSession(scope);
    const pending = current.request;
    if (!pending && branchRecovery.hasPendingRecovery) {
      try { await branchRecovery.retry(); store.updateSession(scope, (value) => ({ ...value, error: undefined })); } catch (error) { store.updateSession(scope, (value) => ({ ...value, error: error instanceof Error ? error.message : "Branch change remains unresolved." })); }
      return;
    }
    if (!pending || current.checkingRecovery) return;
    store.updateSession(scope, (value) => ({ ...value, checkingRecovery: true, error: undefined }));
    try {
      let run: AcpRun | undefined;
      if (pending.kind === "stop") {
        run = await apiFetch<AcpRun>(`/acp/runs/${encodeURIComponent((pending.body as { runId: string }).runId)}`);
        if (!["completed", "failed", "cancelled"].includes(run.status)) throw new Error("The operation is still active. The previous stop outcome remains unresolved.");
      } else {
        const outcome = await apiFetch<components["schemas"]["MutationOutcome"]>(`/mutation-outcomes/${encodeURIComponent(pending.key)}`, { params: { operation: "planning-operation-create" } });
        if (outcome.state !== "completed") throw new Error(outcome.state === "pending" ? "The previous request is still pending. No new request was sent." : "The previous request outcome is still unknown. No new request was sent.");
        const status = outcome.response_status ?? 0;
        const body = outcome.response_body;
        if (status >= 400 && status < 500 && status !== 408 && (body?.error as { code?: string } | undefined)?.code !== "idempotency_pending") {
          store.updateSession(scope, (value) => value.request?.key === pending.key ? { ...value, request: undefined, error: "The previous request was rejected. Review your draft before trying again." } : value);
          return;
        }
        const requestBody = pending.body as CreateAcpRunRequest;
        if (status < 200 || status >= 300 || typeof body?.run_id !== "string" || body.planning_session_id !== sessionId || body.agent_name !== requestBody.agent_name) throw new Error("The stored request outcome could not be verified. No new request was sent.");
        run = await apiFetch<AcpRun>(`/acp/runs/${encodeURIComponent(body.run_id)}`);
        if (run.planning_session_id !== sessionId || run.agent_name !== requestBody.agent_name) throw new Error("The recovered operation does not match this request.");
      }
      if (run.planning_session_id !== sessionId) throw new Error("The recovered operation does not belong to this session.");
      const recovered = run;
      queryClient.setQueryData(plannerKeys.operation(recovered.run_id), recovered);
      store.updateSession(scope, (value) => value.request?.key === pending.key ? { ...value, request: undefined, optimisticOperation: null, trackedOperationId: recovered.run_id, error: undefined } : value);
      store.trackOperation({ ...scope, operationId: recovered.run_id, kind: pending.kind === "stop" ? canonicalOperation?.kind ?? "reply" : pending.kind, sessionTitle: session?.title ?? "", draftBranch: pending.draftBranch });
      if (pending.draft) store.clearSubmittedDraft(scope, pending.draft.branch, pending.draft.generation, pending.draft.kind === "edit");
      void queryClient.invalidateQueries({ queryKey: plannerKeys.detail(sessionId) });
      void queryClient.invalidateQueries({ queryKey: plannerKeys.messages(sessionId) });
    } catch (error) {
      store.updateSession(scope, (value) => ({ ...value, error: error instanceof Error ? error.message : "Could not check the previous request" }));
    } finally {
      store.updateSession(scope, (value) => ({ ...value, checkingRecovery: false }));
    }
  };

  return {
    activeOperation, activeOperationId, connection, recoveryKind, recoveryBlocked, setActiveOperationId,
    recovery: {
      checking: state.checkingRecovery, onCheck: checkRequestOutcome, pendingKind: recoveryKind ?? (branchRecovery.hasPendingRecovery ? "branch" : null),
      onRetry: async () => {
        const pending = store.getSession(scope).request;
        if (!pending && branchRecovery.hasPendingRecovery) { await checkRequestOutcome(); return; }
        if (!pending) return;
        if (pending.kind === "stop") return stopOperation();
        await perform(pending.kind, pending.body as CreateAcpRunRequest, { parentMessageId: pending.parentMessageId });
      },
    },
    createOperation: { ...createOperationMutation, error: state.error ? new Error(state.error) : null },
    operationQueryState: {
      isLoading: Boolean(operationId) && activeOperationQuery.isPending && !activeOperationQuery.data,
      isError: activeOperationQuery.isError, isRetrying: activeOperationQuery.isFetching, error: activeOperationQuery.error,
      retry: () => { void activeOperationQuery.refetch(); },
    },
    stop: { available: Boolean(activeOperationId) && !state.optimisticOperation, pending: state.request?.kind === "stop" && state.request.phase === "submitting", error: state.request?.kind === "stop" ? state.error : undefined, onStop: stopOperation },
    submitReply: async ({ text, answer, parentMessageId, draft }: ReplySubmission) => {
      if (!session) throw new Error("Planning session is required");
      const pending = store.getSession(scope).request;
      if (pending?.kind === "reply") throw new Error("Retry the pending reply request before sending another reply");
      if (pending) throw new Error(`Retry the pending ${pending.kind} request before starting another planner operation`);
      const parent = parentMessageId ?? session.active_leaf_message_id;
      await perform("reply", { agent_name: "planner-reply", planning_session_id: sessionId, reply: { content_markdown: text, ...(answer ? { answer_payload: answer } : {}), ...(parent ? { parent_message_id: parent } : {}) } }, { parentMessageId: parent }, parentMessageId ? undefined : store.branch(scope, session.active_leaf_message_id), draft);
    },
    retryReplyRecovery: async () => {
      const pending = store.getSession(scope).request;
      const body = pending?.body as CreateAcpRunRequest | undefined;
      if (pending?.kind !== "reply" || !body?.reply) throw new Error("No pending reply recovery request is available");
      await perform("reply", body, { parentMessageId: body.reply.parent_message_id });
    },
    regenerate: async ({ messageId, parentMessageId }: { messageId: string; parentMessageId?: string }) => {
      await perform("regenerate", { agent_name: "planner-regenerate", planning_session_id: sessionId, regenerate: { target_message_id: messageId } }, { parentMessageId });
    },
    activateVariant: async ({ messageId }: { messageId: string }) => {
      if (!session) throw new Error("Planning session is required");
      const pending = store.getSession(scope).request;
      if (pending) throw new Error(`Retry the pending ${pending.kind} request before changing the active planner branch`);
      try {
        await branchRecovery.mutateAsync(messageId);
        store.updateSession(scope, (value) => ({ ...value, error: undefined }));
      } catch (error) {
        store.updateSession(scope, (value) => ({ ...value, error: error instanceof Error ? error.message : "Could not switch planner branch" }));
        throw error;
      }
      void queryClient.invalidateQueries({ queryKey: plannerKeys.messages(sessionId) });
      void queryClient.invalidateQueries({ queryKey: plannerKeys.detail(sessionId) });
    },
    startFormalize: () => perform("formalize", { agent_name: "planner-formalize", planning_session_id: sessionId }, {}),
  };
}
