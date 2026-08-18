import { useCallback } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useCreatePlanningSession, type CreatePlanningSessionRequest, type PlanningMessagesPage, type PlanningSessionDetail } from "@/api/hooks";
import { useRecoverableMutation } from "@/lib/use-recoverable-mutation";
import { plannerKeys } from "./planner-query-keys";

function normalizeCreatePlanningSessionRequest(body: CreatePlanningSessionRequest): CreatePlanningSessionRequest {
	const model = body.model_selection?.model?.trim() ?? "";
	const thinkingEffort = body.model_selection?.thinking_effort?.trim() ?? "";
	return {
		...(body.title?.trim() ? { title: body.title.trim() } : {}),
		...(body.external_intake ? { external_intake: body.external_intake } : {}),
		...(model ? { model_selection: {
			model,
			...(thinkingEffort !== "" ? { thinking_effort: thinkingEffort } : {}),
		}} : {}),
	};
}

export function useCreatePlanningSessionNavigation() {
  const queryClient = useQueryClient();
  const createPlanningSession = useCreatePlanningSession();
  const recoverableCreate = useRecoverableMutation(
    "planning-session",
    (body: CreatePlanningSessionRequest, key) => createPlanningSession.mutateAsync({ body, key }),
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

	const mutateAsync = useCallback(async (body: CreatePlanningSessionRequest) => {
		const normalizedBody = normalizeCreatePlanningSessionRequest(body);
		const created = await recoverableCreate.mutateAsync(normalizedBody);
		createPlanningSession.reset();
		return applyCreatedSession(created);
	}, [applyCreatedSession, createPlanningSession, recoverableCreate]);

	const retryRecovery = useCallback(async () => {
		const created = await recoverableCreate.retry();
		createPlanningSession.reset();
		return applyCreatedSession(created);
	}, [applyCreatedSession, createPlanningSession, recoverableCreate]);

  return {
    ...createPlanningSession,
    mutateAsync,
	recoveryRequest: recoverableCreate.pendingRequest,
	hasRecovery: recoverableCreate.hasPendingRecovery,
	retryRecovery,
  };
}
