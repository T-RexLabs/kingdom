import { useMemo } from "react";
import { usePlanningSessionMessagePages, type PlanningMessage } from "@/api/hooks";
import {
  flattenRenderedPlanningMessages,
  getPlanningMessagesTotalCount,
  getRenderedFirstItemIndex,
  PLANNER_MESSAGES_PAGE_SIZE,
} from "./planner-message-pages";

export function usePlannerMessagePages(sessionId: string) {
  const query = usePlanningSessionMessagePages(sessionId, PLANNER_MESSAGES_PAGE_SIZE);

  const data = query.data;
  const messages = useMemo(() => flattenRenderedPlanningMessages(data) as PlanningMessage[], [data]);
  const firstItemIndex = getRenderedFirstItemIndex(data);
  const totalCount = getPlanningMessagesTotalCount(data);

  return {
    ...query,
    messages,
    firstItemIndex,
    totalCount,
    fetchPreviousPage: query.fetchNextPage,
    hasPreviousPage: query.hasNextPage,
    isFetchingPreviousPage: query.isFetchingNextPage,
  };
}
