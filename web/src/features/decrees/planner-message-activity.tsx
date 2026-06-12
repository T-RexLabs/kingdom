import { useMemo } from "react";
import type { AcpRunActivityItem } from "@/api/hooks";
import { useAcpRunActivity } from "@/api/hooks";
import type { PlannerActivityItem } from "./planner-operation-controller";
import { buildPlannerMessageActivityState, type PlannerMessageActivityState } from "./planner-activity-view";

export { type PlannerMessageActivityState } from "./planner-activity-view";

export function usePlannerMessageActivity({
  activityRunId,
  isAssistant,
  isStreaming,
  liveActivityItems,
}: {
  activityRunId?: string;
  isAssistant: boolean;
  isStreaming: boolean;
  liveActivityItems?: PlannerActivityItem[];
}): PlannerMessageActivityState {
  const activityQuery = useAcpRunActivity(activityRunId, {
    enabled: isAssistant && Boolean(activityRunId),
    refetchIntervalMs: isStreaming ? 1_500 : false,
  });

  return useMemo(() => {
    if (!isAssistant) {
      return { currentEvent: null, liveThoughtText: "", thoughts: [], sources: [] };
    }
    return buildPlannerMessageActivityState({
      durableItems: (activityQuery.data?.items ?? []).map(activityItemFromApi),
      liveItems: liveActivityItems,
    });
  }, [activityQuery.data?.items, isAssistant, liveActivityItems]);
}

function activityItemFromApi(item: AcpRunActivityItem): PlannerActivityItem {
  return {
    id: item.id,
    kind: item.kind,
    label: item.label,
    detail: item.detail,
    input: item.input,
    output: item.output,
    occurredAt: item.occurred_at,
  };
}
