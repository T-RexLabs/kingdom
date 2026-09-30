import { useMemo } from "react";
import { useAcpRunActivity } from "@/api/hooks";
import type { ActivityHistoryControls } from "@/features/activity/activity-history";
import {
  activityItemFromApi,
  buildActivityState,
  emptyActivityState,
  type ActivityItem,
  type ActivityState,
} from "@/features/activity/activity-view";

export type PlannerMessageActivityState = ActivityState &
  ActivityHistoryControls & {
    isLoading: boolean;
    error: Error | null;
    retry: () => Promise<unknown>;
  };

export function usePlannerMessageActivity({
  activityRunId,
  isAssistant,
  isStreaming,
  liveActivityItems,
}: {
  activityRunId?: string;
  isAssistant: boolean;
  isStreaming: boolean;
  liveActivityItems?: ActivityItem[];
}): PlannerMessageActivityState {
  const activityQuery = useAcpRunActivity(activityRunId, {
    enabled: isAssistant && Boolean(activityRunId),
    refetchIntervalMs: isStreaming ? 1_500 : false,
  });

  return useMemo(() => {
    if (!isAssistant) {
      return {
        ...emptyActivityState(),
        gaps: [],
        isLoadingMore: false,
        loadError: null,
        loadMore: async () => {},
        isLoading: false,
        error: null,
        retry: async () => {},
      };
    }
    return {
      ...buildActivityState({
        durableItems: (activityQuery.data?.items ?? []).map(activityItemFromApi),
        liveItems: liveActivityItems,
      }),
      gaps: activityQuery.gaps ?? [],
      isLoadingMore: activityQuery.isLoadingMore ?? false,
      loadError: activityQuery.loadError ?? null,
      loadMore: activityQuery.loadMore ?? (async () => {}),
      isLoading: activityQuery.isLoading ?? false,
      error: activityQuery.isError ? activityQuery.error : null,
      retry: activityQuery.refetch,
    };
  }, [
    activityQuery.data?.items,
    activityQuery.gaps,
    activityQuery.isLoadingMore,
    activityQuery.loadError,
    activityQuery.loadMore,
    activityQuery.isLoading,
    activityQuery.isError,
    activityQuery.error,
    activityQuery.refetch,
    isAssistant,
    liveActivityItems,
  ]);
}
