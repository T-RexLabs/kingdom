import { useEffect, useMemo, useRef } from "react";
import { useRunExecutionActivity } from "@/api/hooks-runs";
import type { RunExecution, RunExecutionDetail } from "@/api/hooks";
import {
  activityItemFromApi,
  buildActivityState,
  isConversationActivity,
} from "@/features/activity/activity-view";
import { RunFinalResponse } from "./run-conversation";
import { hasSettledResponse } from "./run-conversation-state";
import { ActivityHistory } from "@/features/activity/activity-history";
import { ActivityDisclosure } from "@/features/activity/activity-disclosure";
import { ActivitySources } from "@/features/activity/activity-sources";
import { selectLiveRunActivity, useLiveRunActivity } from "./run-live-activity";
import {
  isActiveExecutionStatus,
  isQueuedExecutionStatus,
  liveExecutionLabel,
} from "./run-status-semantics";

type RunActivityProps = {
  runId: string;
  attemptId: string | null;
  status?: RunExecution["status"];
  detail?: RunExecutionDetail | null;
  isVisible?: boolean;
  isDetailLoading?: boolean;
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
  isVisible = true,
  isDetailLoading = false,
}: RunActivityProps) {
  const isRunning = isActiveExecutionStatus(status);
  const durable = useRunExecutionActivity(runId, attemptId, {
    refetchIntervalMs: isRunning ? 1_500 : false,
  });
  const activityRef = useRef<HTMLElement>(null);
  const openedAtLatest = useRef(false);
  useEffect(() => {
    if (!isVisible) {
      openedAtLatest.current = false;
      return;
    }
    if (openedAtLatest.current || durable.isLoading || isDetailLoading) return;
    const activity = activityRef.current;
    if (!activity) return;
    activity.scrollIntoView({ block: "end" });
    openedAtLatest.current = true;
  }, [isVisible, durable.isLoading, isDetailLoading, attemptId]);
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
  const conversationItems = useMemo(
    () => state.items.filter(isConversationActivity),
    [state.items],
  );
  const hasHistory = conversationItems.length > 0 || durable.gaps.length > 0;
  // Like a finished planner reply: once the attempt settles with a response,
  // its thinking and tool calls fold away. Without one, the activity is the record.
  const folded = hasFinished && hasSettledResponse(detail);
  const phaseLabel =
    liveExecutionLabel(detail?.progress_reason_code, detail?.progress_event_type) ??
    state.items.filter((item) => item.kind === "progress").at(-1)?.label;
  const showPhase = Boolean(
    phaseLabel &&
      (isQueuedExecutionStatus(status) || (isRunning && !conversationItems.length)),
  );

  if (!attemptId)
    return (
      <p className="py-6 text-sm text-foreground-muted">
        This step has not recorded an attempt yet.
      </p>
    );
  return (
    <section ref={activityRef} className="flex h-full min-h-0 flex-col gap-2" aria-label="Provider activity">
      <h3 className="sr-only">Provider Activity</h3>
      {showPhase && (
        <p role="status" className="py-2 text-sm text-foreground-muted">
          {phaseLabel}
        </p>
      )}
      {durable.isLoading && (
        <p className="py-2 text-xs text-foreground-muted">Loading activity…</p>
      )}
      {durable.isError && (
        <p role="alert" className="text-xs text-status-failed">
          Activity could not be loaded.{" "}
          <button
            type="button"
            className="min-h-9 underline"
            onClick={() => void durable.refetch()}
          >
            Retry Activity
          </button>
        </p>
      )}
      {isRunning && !showPhase && !hasHistory && !durable.isLoading && !durable.isError && (
        <p role="status" className="py-6 text-sm text-foreground-muted">
          Waiting for provider activity.
        </p>
      )}
      {!isRunning &&
        !showPhase &&
        !durable.isLoading &&
        !durable.isError &&
        !conversationItems.length &&
        !hasSettledResponse(detail) && (
          <p className="py-6 text-sm text-foreground-muted">
            {durable.gaps.length > 0
              ? "No thinking or tool calls appear in this loaded window. Load earlier events below."
              : isQueuedExecutionStatus(status)
                ? "This attempt is queued. Provider activity will appear when it starts."
                : hasFinished
                  ? "No thinking, tool activity, or settled response is available for this attempt."
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
      {hasHistory && !folded && (
        <div className="min-h-0 flex-1">
          <ActivityHistory
            fill
            scopeKey={JSON.stringify([runId, attemptId])}
            items={conversationItems}
            gaps={durable.gaps}
            isLoadingMore={durable.isLoadingMore}
            loadError={durable.loadError}
            loadMore={durable.loadMore}
          />
        </div>
      )}
      {/* While the history streams it owns the space; once folded, the work,
          response and sources scroll together. */}
      <div
        className={
          hasHistory && !folded
            ? "max-h-[45%] shrink-0 overflow-y-auto"
            : "min-h-0 flex-1 space-y-2 overflow-y-auto"
        }
      >
        {hasHistory && folded && (
          <ActivityDisclosure
            scopeKey={JSON.stringify([runId, attemptId])}
            items={conversationItems}
            gaps={durable.gaps}
            isLoadingMore={durable.isLoadingMore}
            loadError={durable.loadError}
            loadMore={durable.loadMore}
            isLive={false}
          />
        )}
        <RunFinalResponse key={attemptId} detail={detail} />
        <ActivitySources sources={state.sources} />
      </div>
    </section>
  );
}
