import type { ActivityHistoryControls } from "@/features/activity/activity-history";
import { useMemo } from "react";
import { useAcpRunActivity } from "@/api/hooks";
import {
  activityItemFromApi,
  buildActivityState,
  emptyActivityState,
  type ActivityItem,
  type ActivityState,
} from "@/features/activity/activity-view";

export type { ActivityState as PlannerMessageActivityState } from "@/features/activity/activity-view";

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
}): ActivityState & ActivityHistoryControls {
  const activityQuery = useAcpRunActivity(activityRunId, {
    enabled: isAssistant && Boolean(activityRunId),
    refetchIntervalMs: isStreaming ? 1_500 : false,
  });

  return useMemo(() => {
    if (!isAssistant) {
      return { ...emptyActivityState(), gaps: [], isLoadingMore: false, loadError: null, loadMore: async () => {} };
    }
    return { ...buildActivityState({
      durableItems: (activityQuery.data?.items ?? []).map(activityItemFromApi),
      liveItems: liveActivityItems,
    }), gaps: activityQuery.gaps ?? [], isLoadingMore: activityQuery.isLoadingMore ?? false, loadError: activityQuery.loadError ?? null, loadMore: activityQuery.loadMore ?? (async () => {}) };
  }, [activityQuery.data?.items, activityQuery.gaps, activityQuery.isLoadingMore, activityQuery.loadError, activityQuery.loadMore, isAssistant, liveActivityItems]);
}
