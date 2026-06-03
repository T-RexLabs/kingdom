import { useQueryClient } from "@tanstack/react-query";
import { newIdempotencyKey, useCreatePlanningSession, type CreatePlanningSessionRequest, type PlanningMessagesPage, type PlanningSessionDetail } from "@/api/hooks";
import { plannerKeys } from "./planner-query-keys";

export function useCreatePlanningSessionNavigation() {
  const queryClient = useQueryClient();
  const createPlanningSession = useCreatePlanningSession();

  return {
    ...createPlanningSession,
    mutateAsync: async (body: CreatePlanningSessionRequest) => {
      const created = await createPlanningSession.mutateAsync({
        body,
        key: newIdempotencyKey("planning-session"),
      });
      const session = created.session;
      // Seed the detail cache so the destination page renders without
      // a network round-trip; invalidate the list-prefix so every list
      // slice refetches and picks up the new session via the canonical
      // server response (instead of us synthesising a list summary
      // ourselves and risking schema drift). The new-session-create
      // path is cold-cache by definition for the detail key, so a
      // direct setQueryData is correct here.
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
    },
  };
}
