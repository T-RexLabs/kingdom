import { skipToken, useQuery, type QueryClient } from "@tanstack/react-query";
import { mergeActivityTimeline, type ActivityItem } from "@/features/activity/activity-view";

// Per-attempt live activity buffer. The run event stream is a single connection for
// the whole run, so we key the buffer by attempt id and let each execution view read
// only its own slice. This stays separate from the coarse timeline so high-frequency
// agent reasoning never triggers timeline reconciliation or surface refetches.
export type LiveRunActivityBuffer = Record<string, ActivityItem[]>;

export const liveRunActivityQueryKey = (runId: string) => ["runs", runId, "activity", "live"] as const;

const MAX_LIVE_ACTIVITY_PER_ATTEMPT = 400;
const EMPTY_ACTIVITY: ActivityItem[] = [];

export function useLiveRunActivity(runId: string): LiveRunActivityBuffer {
  const query = useQuery({
    queryKey: liveRunActivityQueryKey(runId),
    queryFn: skipToken,
    enabled: false,
    initialData: {} as LiveRunActivityBuffer,
    staleTime: Number.POSITIVE_INFINITY,
  });
  return query.data ?? {};
}

export function selectLiveRunActivity(buffer: LiveRunActivityBuffer, attemptId?: string | null): ActivityItem[] {
  const trimmed = attemptId?.trim() ?? "";
  if (!trimmed) {
    return EMPTY_ACTIVITY;
  }
  return buffer[trimmed] ?? EMPTY_ACTIVITY;
}

export function appendLiveRunActivity(
  qc: QueryClient,
  runId: string,
  attemptId: string,
  item: ActivityItem,
): void {
  const trimmedAttemptId = attemptId.trim();
  if (!trimmedAttemptId) {
    return;
  }
  qc.setQueryData<LiveRunActivityBuffer>(liveRunActivityQueryKey(runId), (current) => {
    const buffer = current ?? {};
    const existing = buffer[trimmedAttemptId] ?? [];
    const merged = mergeActivityTimeline(existing,[item]);
    const nextItems = merged.slice(-MAX_LIVE_ACTIVITY_PER_ATTEMPT);

    return { ...buffer, [trimmedAttemptId]: nextItems };
  });
}
