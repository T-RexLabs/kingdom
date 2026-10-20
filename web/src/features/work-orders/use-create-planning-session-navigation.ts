import { useCallback, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useCreatePlanningSession, type CreatePlanningSessionRequest, type PlanningMessagesPage, type PlanningSessionDetail } from "@/api/hooks";
import { usePlanningMutationJournal } from "./use-planning-mutation-journal";
import { plannerKeys } from "./planner-query-keys";

function normalizeCreatePlanningSessionRequest(body: CreatePlanningSessionRequest): CreatePlanningSessionRequest {
  const model = body.model_selection?.model?.trim() ?? "";
  const thinkingEffort = body.model_selection?.thinking_effort?.trim() ?? "";
  return {
    ...(body.title?.trim() ? { title: body.title.trim() } : {}),
    ...(model ? { model_selection: {
      model,
      ...(thinkingEffort !== "" ? { thinking_effort: thinkingEffort } : {}),
    }} : {}),
  };
}

export function useCreatePlanningSessionNavigation(workspace: string) {
  const queryClient = useQueryClient();
  const createPlanningSession = useCreatePlanningSession();
  const [resolution, setResolution] = useState<{ pending: boolean; error: Error | null }>({ pending: false, error: null });
  const [isCheckingRecovery, setIsCheckingRecovery] = useState(false);
  const recoverableCreate = usePlanningMutationJournal(
    "planning-session",
    (body: CreatePlanningSessionRequest, key) => createPlanningSession.mutateAsync({ body, key }),
    workspace,
    "planning-session-create",
  );

  const applyCreatedSession = useCallback((created: Awaited<ReturnType<typeof createPlanningSession.mutateAsync>>) => {
    const session = created.session;
    queryClient.setQueryData<PlanningSessionDetail>(plannerKeys.detail(session.planning_session_id), session);
    void queryClient.invalidateQueries({ queryKey: plannerKeys.listPrefix() });
    if (created.opening_message) {
      queryClient.setQueryData(plannerKeys.messages(session.planning_session_id), {
        pages: [
          {
            planning_session_id: session.planning_session_id,
            items: [created.opening_message],
            total_count: 1,
            first_item_index: 0,
            previous_cursor: undefined,
          },
        ],
        pageParams: [undefined],
      } satisfies { pages: PlanningMessagesPage[]; pageParams: Array<string | undefined> });
    }
    return session;
  }, [createPlanningSession, queryClient]);

  const resolveCreate = useCallback(async (action: () => ReturnType<typeof createPlanningSession.mutateAsync>) => {
    setResolution({ pending: true, error: null });
    try {
      const created = await action();
      createPlanningSession.reset();
      const session = applyCreatedSession(created);
      setResolution({ pending: false, error: null });
      return session;
    } catch (error) {
      setResolution({ pending: false, error: error instanceof Error ? error : new Error("The previous session request remains unresolved.") });
      throw error;
    }
  }, [applyCreatedSession, createPlanningSession]);

  const mutateAsync = useCallback((body: CreatePlanningSessionRequest) => resolveCreate(() => recoverableCreate.mutateAsync(normalizeCreatePlanningSessionRequest(body))), [resolveCreate, recoverableCreate]);
  const retryRecovery = useCallback(async () => {
    setIsCheckingRecovery(true);
    try {
      return await resolveCreate(recoverableCreate.retry);
    } finally {
      setIsCheckingRecovery(false);
    }
  }, [resolveCreate, recoverableCreate.retry]);

  return {
    ...createPlanningSession,
    isPending: createPlanningSession.isPending || resolution.pending,
    isCheckingRecovery,
    error: resolution.error ?? createPlanningSession.error,
    mutateAsync,
  recoveryRequest: recoverableCreate.pendingRequest,
  hasRecovery: recoverableCreate.hasPendingRecovery,
  retryRecovery,
  };
}
