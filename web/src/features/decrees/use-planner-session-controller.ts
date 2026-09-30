import { useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  type CreateAcpRunRequest,
  newIdempotencyKey,
  useActivateVariant,
  useCreatePlanningOperation,
  usePlanningOperation,
  type PlanningSessionDetail,
} from "@/api/hooks";
import type { InfiniteData, QueryClient } from "@tanstack/react-query";
import type { PlanningMessagesPage } from "@/api/hooks";
import { isPlannerOperationTerminal, type AnswerPayload, type PlannerOperationState } from "./planner-operation-controller";
import { usePlannerOperation } from "./use-planner-operation";
import { applyPlanningSessionFields } from "./planner-session-cache";
import { plannerKeys } from "./planner-query-keys";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";

/**
 * Coordinates the decree-planning page's optimistic mutation flow around one
 * canonical planning session.
 *
 * The hook intentionally separates three layers of truth:
 *
 * 1. The server-attested session and operation rows from React Query.
 * 2. A short-lived optimistic operation shell that keeps reply/regenerate/
 *    formalize interactions responsive before the server has allocated the real
 *    operation id.
 * 3. A field-scoped rollback snapshot that restores only the cache slices this
 *    hook mutated if a request fails, so concurrent SSE or sibling cache writes
 *    survive untouched.
 *
 * Request sequencing is latest-request-wins within the mounted controller.
 * `requestSeqRef` fences every async completion path so an older rejected or
 * fulfilled mutation cannot clobber the optimistic state created by a newer
 * user action. Once the server creates an operation, `usePlannerOperation`
 * takes over streaming and terminal-state reconciliation; this hook's job is
 * limited to composing mutations, optimistic UI, and precise rollback.
 */

/**
 * Snapshot of state we mutate optimistically before a mutation, so
 * that we can roll back atomically if the mutation rejects. Without
 * this, a network failure leaves the composer disabled, the
 * transcript truncated, and the conversation_state stuck on
 * "processing" until the user reloads.
 *
 * `sessionBefore` carries ONLY the session-detail fields the caller
 * actually mutated optimistically. Restoring a full session snapshot
 * would clobber unrelated fields that SSE / sibling reconciliation
 * may have written to the cache between snapshot capture and rollback
 * (the same stale-snapshot class field-scoped writes elsewhere
 * already eliminated). Live cache values — not the closure-captured
 * `session` prop — are the actual rollback target, so takeSnapshot
 * reads from `qc.getQueryData(plannerKeys.detail(...))`.
 */
type Snapshot = {
  messages: InfiniteData<PlanningMessagesPage, unknown> | undefined;
  sessionId: string;
  sessionBefore: Partial<PlanningSessionDetail>;
  optimistic: PlannerOperationState | null;
};

export function usePlannerSessionController(session: PlanningSessionDetail | undefined) {
  const queryClient = useQueryClient();
  const sessionId = session?.planning_session_id ?? "";
  const createOperation = useCreatePlanningOperation(sessionId);
  const recoverableReply = useRecoverableMutation(
    "planning-reply",
    (body: CreateAcpRunRequest, key) => createOperation.mutateAsync({ body, key }),
    sessionId,
  );
  const recoverableRegenerate = useRecoverableMutation(
    "planning-regenerate",
    (body: CreateAcpRunRequest, key) => createOperation.mutateAsync({ body, key }),
    sessionId,
  );
  const recoverableFormalize = useRecoverableMutation(
    "planning-formalize",
    (body: CreateAcpRunRequest, key) => createOperation.mutateAsync({ body, key }),
    sessionId,
  );
  const recoveryKind = recoverableReply.hasPendingRecovery
    ? "reply"
    : recoverableRegenerate.hasPendingRecovery
      ? "regenerate"
      : recoverableFormalize.hasPendingRecovery
        ? "formalize"
        : null;
  const activateVariantMutation = useActivateVariant(sessionId);
  const activateVariantRef = useRef(activateVariantMutation);
  useEffect(() => { activateVariantRef.current = activateVariantMutation; });
  const [trackedOperationId, setTrackedOperationId] = useState<string | undefined>(undefined);
  const [optimisticOperation, setOptimisticOperation] = useState<PlannerOperationState | null>(null);
  const requestSeqRef = useRef(0);

  const operationId = trackedOperationId ?? session?.current_run_id ?? undefined;
  const activeOperationQuery = usePlanningOperation(operationId);
  const { operation: streamedOperation, connection } = usePlannerOperation(activeOperationQuery.data ?? null);
  const canonicalOperation = streamedOperation?.operationId === operationId ? streamedOperation : null;
  const activeOperation = optimisticOperation ?? canonicalOperation;
  const activeOperationId = trackedOperationId && !isPlannerOperationTerminal(activeOperation?.status ?? "pending")
    ? trackedOperationId
    : undefined;
  const setActiveOperationId = useCallback((next: string | undefined) => {
    setTrackedOperationId(next);
  }, []);

  useEffect(() => {
    if (!canonicalOperation || !isPlannerOperationTerminal(canonicalOperation.status)) return;

    // Failed and cancelled runs stay query-backed for their notice. Successful
    // runs release local tracking after the stream hook reconciles the canonical
    // transcript. Defer the query-to-local synchronization so terminal cache
    // reconciliation completes without a cascading render inside this effect.
    let mounted = true;
    queueMicrotask(() => {
      if (!mounted) return;
      setTrackedOperationId((current) => {
        if (canonicalOperation.status === "error" || canonicalOperation.status === "cancelled") {
          return canonicalOperation.operationId;
        }
        return current === canonicalOperation.operationId ? undefined : current;
      });
    });
    return () => { mounted = false; };
  }, [canonicalOperation]);

  const operationQueryState = useMemo(() => ({
    isLoading: Boolean(operationId) && activeOperationQuery.isPending && !activeOperationQuery.data,
    isError: activeOperationQuery.isError,
    isRetrying: activeOperationQuery.isFetching,
    error: activeOperationQuery.error,
    retry: () => {
      void activeOperationQuery.refetch();
    },
  }), [activeOperationQuery, operationId]);

  const state = useMemo(
    () => ({
      activeOperation,
      activeOperationId,
      connection,
      operationQueryState,
      setActiveOperationId,
      createOperation,
      recoveryKind,
      submitReply: async ({ text, answer, parentMessageId }: { text: string; answer?: AnswerPayload; parentMessageId?: string }) => {
        if (!session) throw new Error("Planning session is required");
        if (recoveryKind && recoveryKind !== "reply") throw new Error(`Retry the pending ${recoveryKind} request before starting another planner operation`);
        requestSeqRef.current += 1;
        const seq = requestSeqRef.current;
        const snap = takeSnapshot(queryClient, session.planning_session_id, ["conversation_state"], optimisticOperation);
        const now = new Date().toISOString();
        // The new user message branches off the explicit parent (editing an
        // earlier message) or, by default, the session's active leaf (a normal
        // reply). Nothing is truncated; the server moves the active leaf so the
        // new branch loads on the next messages refetch.
        const branchParentId = parentMessageId ?? session.active_leaf_message_id ?? undefined;
        setOptimisticOperation({
          kind: "reply",
          operationId: `pending-operation-${seq}`,
          status: "pending",
          lastEventSeq: 0,
          parentMessageId: branchParentId,
          userMessageId: `pending-user-${seq}`,
          userCreatedAt: now,
          assistantMessageId: `pending-assistant-${seq}`,
          assistantCreatedAt: now,
          pendingReply: { contentMarkdown: text, answer },
          replyText: "",
          formalizeText: "",
          error: null,
          events: [],
          activityItems: [],
        });
        applyPlanningSessionFields(queryClient, session.planning_session_id, {
          conversation_state: "processing",
        });

        try {
          const created = await recoverableReply.mutateAsync({
              agent_name: "planner-reply",
              planning_session_id: session.planning_session_id,
              reply: {
                content_markdown: text,
                ...(answer ? { answer_payload: answer } : {}),
                ...(parentMessageId ? { parent_message_id: parentMessageId } : {}),
              },
            } satisfies CreateAcpRunRequest);
          if (seq !== requestSeqRef.current) return;
          setOptimisticOperation(null);
          setActiveOperationId(created.run_id);
          // The server appended the user message and moved the active leaf;
          // refetch so the new branch (user turn) replaces the optimistic ghost.
          void queryClient.invalidateQueries({ queryKey: plannerKeys.messages(session.planning_session_id) });
        } catch (err) {
          if (seq !== requestSeqRef.current) return;
          restoreSnapshot(queryClient, snap, setOptimisticOperation);
          throw err;
        }
      },
      regenerate: async ({ messageId, parentMessageId }: { messageId: string; parentMessageId?: string }) => {
        if (!session) throw new Error("Planning session is required");
        if (recoveryKind && recoveryKind !== "regenerate") throw new Error(`Retry the pending ${recoveryKind} request before starting another planner operation`);
        requestSeqRef.current += 1;
        const seq = requestSeqRef.current;
        const snap = takeSnapshot(queryClient, session.planning_session_id, ["conversation_state"], optimisticOperation);
        const now = new Date().toISOString();
        setOptimisticOperation({
          kind: "regenerate",
          operationId: `pending-regenerate-${seq}`,
          status: "pending",
          lastEventSeq: 0,
          // The new answer branches off the same parent as the message it
          // regenerates; the prior answer is preserved as a sibling.
          parentMessageId,
          assistantMessageId: `pending-regenerate-assistant-${seq}`,
          assistantCreatedAt: now,
          pendingReply: null,
          replyText: "",
          formalizeText: "",
          error: null,
          events: [],
          activityItems: [],
        });
        applyPlanningSessionFields(queryClient, session.planning_session_id, {
          conversation_state: "processing",
        });
        try {
          const created = await recoverableRegenerate.mutateAsync({
              agent_name: "planner-regenerate",
              planning_session_id: session.planning_session_id,
              regenerate: { target_message_id: messageId },
            } satisfies CreateAcpRunRequest);
          if (seq !== requestSeqRef.current) return;
          setOptimisticOperation(null);
          setActiveOperationId(created.run_id);
          // Server moved the active leaf to the fork point; refetch so the
          // active path reflects the regenerated branch.
          void queryClient.invalidateQueries({ queryKey: plannerKeys.messages(session.planning_session_id) });
        } catch (err) {
          if (seq !== requestSeqRef.current) return;
          restoreSnapshot(queryClient, snap, setOptimisticOperation);
          throw err;
        }
      },
      activateVariant: async ({ messageId }: { messageId: string }) => {
        if (!session) throw new Error("Planning session is required");
        if (recoveryKind) throw new Error(`Retry the pending ${recoveryKind} request before changing the active planner branch`);
        await activateVariantRef.current.mutateAsync({
          value: messageId,
          key: newIdempotencyKey("activate-variant"),
        });
        void queryClient.invalidateQueries({ queryKey: plannerKeys.messages(session.planning_session_id) });
        void queryClient.invalidateQueries({ queryKey: plannerKeys.detail(session.planning_session_id) });
      },
      startFormalize: async () => {
        if (!session) throw new Error("Planning session is required");
        if (recoveryKind && recoveryKind !== "formalize") throw new Error(`Retry the pending ${recoveryKind} request before starting another planner operation`);
        requestSeqRef.current += 1;
        const seq = requestSeqRef.current;
        // Formalize doesn't touch session detail in the pre-success
        // path — the applyPlanningSessionFields call sits *after*
        // mutateAsync resolves. Empty field list means
        // restoreSnapshot's detail revert is a no-op via the helper's
        // empty-object guard, but we still capture the messages
        // cache in case the call sites grow optimistic message
        // writes.
        const snap = takeSnapshot(queryClient, session.planning_session_id, [], optimisticOperation);
        setOptimisticOperation({
          kind: "formalize",
          operationId: `pending-formalize-${seq}`,
          status: "pending",
          lastEventSeq: 0,
          pendingReply: null,
          replyText: "",
          formalizeText: "",
          error: null,
          events: [],
          activityItems: [],
        });
        try {
          const created = await recoverableFormalize.mutateAsync({
              agent_name: "planner-formalize",
              planning_session_id: session.planning_session_id,
            } satisfies CreateAcpRunRequest);
          if (seq !== requestSeqRef.current) return created;
          setOptimisticOperation(null);
          setActiveOperationId(created.run_id);
          applyPlanningSessionFields(queryClient, session.planning_session_id, {
            current_run_id: created.run_id,
            conversation_state: "processing",
          });
          return created;
        } catch (err) {
          if (seq !== requestSeqRef.current) throw err;
          restoreSnapshot(queryClient, snap, setOptimisticOperation);
          throw err;
        }
      },
    }),
    [activeOperation, activeOperationId, connection, createOperation, operationQueryState, optimisticOperation, queryClient, recoverableFormalize, recoverableRegenerate, recoverableReply, recoveryKind, session, setActiveOperationId],
  );

  return state;
}

function takeSnapshot(
  queryClient: QueryClient,
  sessionId: string,
  fieldNames: readonly (keyof PlanningSessionDetail)[],
  optimistic: PlannerOperationState | null,
): Snapshot {
  // Read the LIVE cache, not the closure-captured session prop. The
  // prop is usually in sync with the cache, but a same-tick SSE patch
  // can land between prop capture and the optimistic write — the
  // cache value is the actual rollback target.
  const liveDetail = queryClient.getQueryData<PlanningSessionDetail>(plannerKeys.detail(sessionId));
  const sessionBefore: Partial<PlanningSessionDetail> = {};
  if (liveDetail) {
    // Generic-key indexing on Partial<T> trips TypeScript's variance
    // checks even when the indexed value is structurally compatible;
    // the Record cast keeps the runtime semantics identical while
    // satisfying the type checker (same pattern already in use in
    // planner-session-cache.ts's applyPlanningSessionFields).
    for (const name of fieldNames) {
      (sessionBefore as Record<string, unknown>)[name as string] =
        (liveDetail as Record<string, unknown>)[name as string];
    }
  }
  return {
    messages: queryClient.getQueryData(plannerKeys.messages(sessionId)),
    sessionId,
    sessionBefore,
    optimistic,
  };
}

function restoreSnapshot(
  queryClient: QueryClient,
  snap: Snapshot,
  setOptimistic: (next: PlannerOperationState | null) => void,
) {
  queryClient.setQueryData(plannerKeys.messages(snap.sessionId), snap.messages);
  // Field-scoped revert: only the fields the caller declared at
  // takeSnapshot time get rewound. Concurrent SSE/sibling
  // reconciliation patches to other fields survive untouched.
  // applyPlanningSessionFields no-ops on an empty fields object,
  // so the formalize call site (which doesn't touch session detail
  // pre-mutate-success) skips the detail write entirely.
  applyPlanningSessionFields(queryClient, snap.sessionId, snap.sessionBefore);
  setOptimistic(snap.optimistic);
}
