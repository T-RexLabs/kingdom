import { useRunExecutionActivity } from "@/api/hooks-runs";
import { selectLiveRunActivity, useLiveRunActivity } from "./run-live-activity";

// useNodeActivityRecency returns the newest ACP activity timestamp for a node attempt
// — the honest "last time the agent did something" — merging the durable activity
// endpoint with the live buffer. This is the liveness signal (replacing the session
// lease heartbeat), reusing the same queries the Activity feed already loads.
export function useNodeActivityRecency(
  runId: string,
  attemptId: string | null,
  isRunning: boolean,
): string | undefined {
  const durable = useRunExecutionActivity(runId, attemptId, {
    refetchIntervalMs: isRunning ? 1_500 : false,
  });
  const liveItems = selectLiveRunActivity(useLiveRunActivity(runId), attemptId);

  let latest: string | undefined;
  for (const item of durable.data?.items ?? []) {
    if (item.occurred_at && (!latest || item.occurred_at > latest)) {
      latest = item.occurred_at;
    }
  }
  for (const item of liveItems) {
    if (item.occurredAt && (!latest || item.occurredAt > latest)) {
      latest = item.occurredAt;
    }
  }
  return latest;
}
