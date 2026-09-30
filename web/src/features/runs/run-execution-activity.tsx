import { useMemo } from "react";
import { useRunExecutionActivity } from "@/api/hooks-runs";
import type { RunExecution, RunExecutionDetail } from "@/api/hooks";
import {
  activityItemFromApi,
  buildActivityState,
} from "@/features/activity/activity-view";
import { RunFinalResponse } from "./run-conversation";
import { hasSettledResponse } from "./run-conversation-state";
import { ActivityDisclosure } from "@/features/activity/activity-disclosure";
import { selectLiveRunActivity, useLiveRunActivity } from "./run-live-activity";
import {
  isActiveExecutionStatus,
  isQueuedExecutionStatus,
} from "./run-status-semantics";

type RunActivityProps = {
  runId: string;
  attemptId: string | null;
  status?: RunExecution["status"];
  detail?: RunExecutionDetail | null;
};
export function RunExecutionActivity(props: RunActivityProps) {
  return (
    <RunAttemptActivity
      key={JSON.stringify([props.runId, props.attemptId])}
      {...props}
    />
  );
}
function RunAttemptActivity({
  runId,
  attemptId,
  status,
  detail,
}: RunActivityProps) {
  const isRunning = isActiveExecutionStatus(status);
  const durable = useRunExecutionActivity(runId, attemptId, {
    refetchIntervalMs: isRunning ? 1_500 : false,
  });
  const liveBuffer = useLiveRunActivity(runId);
  const liveItems = selectLiveRunActivity(liveBuffer, attemptId);
  const state = useMemo(
    () =>
      buildActivityState({
        durableItems: (durable.data?.items ?? []).map(activityItemFromApi),
        liveItems,
      }),
    [durable.data?.items, liveItems],
  );

  const hasFinished =
    status === "succeeded" || status === "failed" || status === "cancelled";

  if (!attemptId)
    return (
      <p className="py-6 text-sm text-foreground-muted">
        This step has not recorded an attempt yet.
      </p>
    );
  return (
    <section className="space-y-2" aria-label="Provider activity">
      <h3 className="sr-only">Provider Activity</h3>
      {!durable.isLoading &&
        !durable.isError &&
        !state.items.length &&
        !hasSettledResponse(detail) && (
          <p className="py-6 text-sm text-foreground-muted">
            {durable.gaps.length > 0
              ? "No messages, thoughts, or tool calls appear in this loaded window. Open activity to load earlier history."
              : isQueuedExecutionStatus(status)
                ? "This attempt is queued. Provider activity will appear when it starts."
                : isRunning
                  ? "Waiting for provider activity."
                  : hasFinished
                    ? "No provider messages, thoughts, or tool activity were recorded for this attempt."
                    : "No provider activity has been recorded yet."}
          </p>
        )}
      {liveItems.some(
        (item) => item.sequence > (durable.data?.snapshotThrough ?? 0),
      ) && (
        <p className="text-xs text-foreground-muted">
          New live activity is awaiting durable reconciliation.
        </p>
      )}
      <ActivityDisclosure
        scopeKey={JSON.stringify([runId, attemptId])}
        items={state.items}
        gaps={durable.gaps}
        isLoading={durable.isLoading}
        error={durable.error}
        retry={() => durable.refetch()}
        isLoadingMore={durable.isLoadingMore}
        loadError={durable.loadError}
        loadMore={durable.loadMore}
      />
      <RunFinalResponse key={attemptId} detail={detail} />
    </section>
  );
}
