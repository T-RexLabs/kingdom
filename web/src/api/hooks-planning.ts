import { skipToken, useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, apiFetch } from "./client";
import { manualRefetchQueryOptions } from "./query-freshness";
import type {
  AcpRun,
  AcpRunActivityResponse,
  CreateAcpRunRequest,
  CreatePlanningSessionRequest,
  CreatePlanningSessionResult,
  MutationKeyOnly,
  MutationWithBody,
  MutationWithValue,
  PlanningDraft,
  PlanningMessage,
  PlanningMessageVariantsResponse,
  PlanningMessagesPage,
  PlanningSessionDetail,
  PlanningSessionListResponse,
  UpdatePlanningSessionRequest,
} from "./hooks-types";
import { applyPlanningSessionFields } from "@/features/decrees/planner-session-cache";
import { plannerKeys } from "@/features/decrees/planner-query-keys";
import { useSlicedMutation } from "./use-sliced-mutation";

const plannerQueryOptions = {
  staleTime: 30_000,
  refetchOnMount: manualRefetchQueryOptions.refetchOnMount,
  refetchOnReconnect: manualRefetchQueryOptions.refetchOnReconnect,
  refetchOnWindowFocus: manualRefetchQueryOptions.refetchOnWindowFocus,
} as const;

export const ACP_RUN_REFETCH_INTERVAL_MS = 10_000;

export function usePlanningSessions(limit = 100) {
  return useQuery({
    queryKey: plannerKeys.list(limit),
    queryFn: () => apiFetch<PlanningSessionListResponse>("/planning-sessions", { params: { limit } }),
    ...plannerQueryOptions,
  });
}

export function usePlanningSession(sessionId?: string) {
  return useQuery({
    // Disabled queries get a sentinel key so the cache does not hold a
    // `["planning-sessions", ""]` entry that could shadow real lookups.
    queryKey: sessionId ? plannerKeys.detail(sessionId) : plannerKeys.pendingDetail(),
    queryFn: sessionId
      ? () => apiFetch<PlanningSessionDetail>(`/planning-sessions/${sessionId}`)
      : skipToken,
    ...plannerQueryOptions,
  });
}

export function usePlanningSessionMessagePages(sessionId?: string, limit = 50) {
  return useInfiniteQuery({
    queryKey: sessionId ? plannerKeys.messages(sessionId, limit) : plannerKeys.pendingMessages(),
    queryFn: sessionId
      ? ({ pageParam }: { pageParam?: string }) =>
          apiFetch<PlanningMessagesPage>(`/planning-sessions/${sessionId}/messages`, {
            params: {
              limit,
              before: typeof pageParam === "string" && pageParam.trim() ? pageParam : undefined,
            },
          })
      : skipToken,
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage) => lastPage.previous_cursor ?? undefined,
    ...plannerQueryOptions,
  });
}

export function usePlanningSessionDraft(sessionId?: string) {
  return useQuery({
    queryKey: sessionId ? plannerKeys.plan(sessionId) : plannerKeys.pendingPlan(),
    queryFn: sessionId
      ? async () => {
          try {
            return await apiFetch<PlanningDraft | null>(`/planning-sessions/${sessionId}/plan`);
          } catch (error) {
            if (error instanceof ApiError && error.status === 404) {
              return null;
            }
            throw error;
          }
        }
      : skipToken,
    ...plannerQueryOptions,
  });
}

export function useCreatePlanningSession() {
  return useMutation({
    mutationFn: ({ body, key }: MutationWithBody<CreatePlanningSessionRequest>) =>
      apiFetch<CreatePlanningSessionResult>("/planning-sessions", {
        method: "POST",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify(body),
      }),
  });
}

// Title and model-selection updates are split into two narrow hooks so
// each owns an independent mutation `error` slot. Sharing one hook surfaced
// title-save failures through every consumer of `error` and gave the silent
// model-autosave path no way to surface its own failures without dragging the
// title chip in too. Strict body typing on each hook also stops callers from
// accidentally PATCHing fields outside their slice.
export function useUpdatePlanningSessionTitle(sessionId: string) {
  const qc = useQueryClient();
  return useSlicedMutation<{ title: string; key: string }, PlanningSessionDetail>(
    ({ title, key }, signal) =>
      apiFetch<PlanningSessionDetail>(`/planning-sessions/${sessionId}`, {
        method: "PATCH",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify({ title }),
        signal,
      }),
    (updated) => {
      applyPlanningSessionFields(qc, sessionId, {
        title: updated.title,
        title_state: updated.title_state,
      });
    },
    sessionId,
  );
}

export function useUpdatePlanningSessionModelSelection(sessionId: string) {
  const qc = useQueryClient();
  return useSlicedMutation<
    {
      modelSelection: UpdatePlanningSessionRequest["model_selection"];
      key: string;
      editGeneration?: number;
    },
    PlanningSessionDetail
  >(
    ({ modelSelection, key }, signal) =>
      apiFetch<PlanningSessionDetail>(`/planning-sessions/${sessionId}`, {
        method: "PATCH",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify({ model_selection: modelSelection }),
        signal,
      }),
    (updated) => {
      applyPlanningSessionFields(qc, sessionId, {
        model_selection: updated.model_selection,
      });
    },
    sessionId,
  );
}

export function useGeneratePlanningSessionTitle(sessionId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ key }: MutationKeyOnly) =>
      apiFetch<AcpRun>(`/acp/runs`, {
        method: "POST",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify({
          agent_name: "planner-generate-title",
          planning_session_id: sessionId,
        } satisfies CreateAcpRunRequest),
      }),
    onSuccess: (run) => {
      applyPlanningSessionFields(qc, sessionId, {
        current_title_run_id: run.run_id,
      });
      void qc.invalidateQueries({ queryKey: plannerKeys.detail(sessionId) });
    },
  });
}

export function useDeletePlanningSession() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ value, key }: MutationWithValue<string>) =>
      apiFetch<void>(`/planning-sessions/${value}`, {
        method: "DELETE",
        headers: { "Idempotency-Key": key },
      }),
    onSuccess: (_data, { value: sessionId }) => {
      qc.invalidateQueries({ queryKey: plannerKeys.all() });
      qc.removeQueries({ queryKey: plannerKeys.detail(sessionId) });
      qc.removeQueries({ queryKey: plannerKeys.messages(sessionId) });
      qc.removeQueries({ queryKey: plannerKeys.plan(sessionId) });
    },
  });
}

export function useCreatePlanningOperation(sessionId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ body, key }: MutationWithBody<CreateAcpRunRequest>) =>
      apiFetch<AcpRun>(`/acp/runs`, {
        method: "POST",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify(body),
      }),
    onSuccess: (run) => {
      qc.setQueryData<AcpRun>(plannerKeys.operation(run.run_id), run);
      applyPlanningSessionFields(qc, sessionId, {
        conversation_state: "processing",
        current_run_id: run.run_id,
      });
      void qc.invalidateQueries({ queryKey: plannerKeys.detail(sessionId) });
    },
  });
}

export function usePlanningOperation(operationId?: string) {
  return useQuery({
    queryKey: operationId ? plannerKeys.operation(operationId) : plannerKeys.pendingOperation(),
    queryFn: operationId
      ? () => apiFetch<AcpRun>(`/acp/runs/${operationId}`)
      : skipToken,
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status === "completed" || status === "failed" || status === "cancelled"
        ? false
        : ACP_RUN_REFETCH_INTERVAL_MS;
    },
    refetchIntervalInBackground: true,
  });
}

export function useAcpRunActivity(
  runId?: string,
  options?: { enabled?: boolean; refetchIntervalMs?: number | false },
) {
  const enabled = options?.enabled ?? true;
  return useQuery({
    queryKey: runId ? plannerKeys.activity(runId) : ["planning-operations", "__pending__", "activity"],
    queryFn: runId
      ? () => apiFetch<AcpRunActivityResponse>(`/acp/runs/${runId}/activity`)
      : skipToken,
    enabled: enabled && Boolean(runId),
    refetchInterval: options?.refetchIntervalMs,
    ...plannerQueryOptions,
  });
}


export function useActivateVariant(sessionId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ value, key }: MutationWithValue<string>) =>
      apiFetch<PlanningMessage>(`/planning-sessions/${sessionId}/messages/${value}/activate`, {
        method: "PUT",
        headers: { "Idempotency-Key": key },
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: plannerKeys.messages(sessionId) });
      qc.invalidateQueries({ queryKey: plannerKeys.detail(sessionId) });
      qc.invalidateQueries({ queryKey: ["planning-sessions", sessionId, "variants"] });
    },
  });
}

export function useListMessageVariants(sessionId: string, messageId?: string) {
  return useQuery({
    queryKey: messageId ? plannerKeys.variants(sessionId, messageId) : plannerKeys.pendingVariants(),
    queryFn: messageId
      ? () => apiFetch<PlanningMessageVariantsResponse>(`/planning-sessions/${sessionId}/messages/${messageId}/variants`)
      : skipToken,
    ...plannerQueryOptions,
  });
}
