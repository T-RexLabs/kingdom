import { useCallback, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { newIdempotencyKey, useCreatePlanningSession, type CreatePlanningSessionRequest, type PlanningMessagesPage, type PlanningSessionDetail } from "@/api/hooks";
import { plannerKeys } from "./planner-query-keys";
import { isAmbiguousMutationFailure } from "./mutation-recovery";

type PlanningSessionRecoveryRequest = {
	body: CreatePlanningSessionRequest;
	key: string;
};

function normalizeCreatePlanningSessionRequest(body: CreatePlanningSessionRequest): CreatePlanningSessionRequest {
	const model = body.model_selection?.model?.trim() ?? "";
	const thinkingEffort = body.model_selection?.thinking_effort?.trim() ?? "";
	if (model === "") {
		return {};
	}
	return {
		model_selection: {
			model,
			...(thinkingEffort !== "" ? { thinking_effort: thinkingEffort } : {}),
		},
	};
}

function planningSessionRecoveryMatchesBody(
	recoveryRequest: PlanningSessionRecoveryRequest | null,
	body: CreatePlanningSessionRequest,
): boolean {
	if (!recoveryRequest) {
		return false;
	}
	const normalizedBody = normalizeCreatePlanningSessionRequest(body);
	const normalizedRecoveryBody = normalizeCreatePlanningSessionRequest(recoveryRequest.body);
	return JSON.stringify(normalizedRecoveryBody) === JSON.stringify(normalizedBody);
}

export function useCreatePlanningSessionNavigation() {
  const queryClient = useQueryClient();
  const createPlanningSession = useCreatePlanningSession();
  const [recoveryRequest, setRecoveryRequest] = useState<PlanningSessionRecoveryRequest | null>(null);

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
		const request = planningSessionRecoveryMatchesBody(recoveryRequest, normalizedBody) && recoveryRequest ? recoveryRequest : {
			body: normalizedBody,
			key: newIdempotencyKey("planning-session"),
		};
		try {
			const created = await createPlanningSession.mutateAsync(request);
			setRecoveryRequest(null);
			createPlanningSession.reset();
			return applyCreatedSession(created);
		} catch (error) {
			if (isAmbiguousMutationFailure(error)) {
				setRecoveryRequest(request);
			} else {
				setRecoveryRequest(null);
			}
			throw error;
		}
	}, [applyCreatedSession, createPlanningSession, recoveryRequest]);

	const retryRecovery = useCallback(async () => {
		if (!recoveryRequest) {
			throw new Error("planner session recovery request is not available");
		}
		const created = await createPlanningSession.mutateAsync(recoveryRequest);
		setRecoveryRequest(null);
		createPlanningSession.reset();
		return applyCreatedSession(created);
	}, [applyCreatedSession, createPlanningSession, recoveryRequest]);

	const discardRecovery = useCallback(() => {
		setRecoveryRequest(null);
		createPlanningSession.reset();
	}, [createPlanningSession]);

	const canRecoverWith = useCallback((body: CreatePlanningSessionRequest) => (
		planningSessionRecoveryMatchesBody(recoveryRequest, body)
	), [recoveryRequest]);

  return {
    ...createPlanningSession,
    mutateAsync,
	recoveryRequest,
	hasRecovery: recoveryRequest !== null,
	canRecoverWith,
	retryRecovery,
	discardRecovery,
  };
}
