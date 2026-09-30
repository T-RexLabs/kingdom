import { useMemo, useState } from "react";
import { useRunExecutionActivity } from "@/api/hooks-runs";
import type { RunExecution, RunExecutionDetail } from "@/api/hooks";
import {
  activityItemFromApi,
  buildActivityState,
} from "@/features/activity/activity-view";
import {
  RunFinalResponse,
  type RunResponseDisclosure,
} from "./run-conversation";
import {
  hasSettledResponse,
  repeatsSettledResponse,
} from "./run-conversation-state";
import { ActivityHistory } from "@/features/activity/activity-history";
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
  const [responseDisclosure, setResponseDisclosure] =
    useState<RunResponseDisclosure>({ response: false, source: false });
  const isRunning = isActiveExecutionStatus(status);
  const [showProgress, setShowProgress] = useState(false);
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

  const visibleItems = showProgress
    ? state.items
    : state.items.filter((item) => item.kind !== "progress");
  const hiddenProgress = state.items.length - visibleItems.length;
  const hasFinished =
    status === "succeeded" || status === "failed" || status === "cancelled";
  const viewOptions = (
    <details className="max-w-lg text-xs text-foreground-muted">
      <summary className="min-h-9 cursor-pointer py-2 font-medium">
        View Options
      </summary>
      <div className="space-y-2 rounded-lg border border-border bg-surface p-3">
        <p>
          {visibleItems.length} Shown · {state.items.length} Loaded
        </p>
        <label className="flex min-h-10 cursor-pointer items-center gap-2">
          <input
            type="checkbox"
            checked={showProgress}
            onChange={(event) => setShowProgress(event.target.checked)}
          />
          Routine Progress
        </label>
        {hiddenProgress > 0 && (
          <p>{hiddenProgress} routine progress events hidden.</p>
        )}
        <p>
          Messages and activity are provider observations. Only the final
          response marked by Kingdom settlement is canonical; activity cannot
          approve or complete a run.
        </p>
      </div>
    </details>
  );

  if (!attemptId)
    return (
      <p className="rounded-xl border border-border p-6 text-sm text-foreground-muted">
        This step has not recorded an attempt yet.
      </p>
    );
  return (
    <section className="space-y-2" aria-label="Provider activity">
      <h3 className="sr-only">Provider Activity</h3>
      {durable.isLoading && (
        <p className="text-sm text-foreground-muted">Loading activity…</p>
      )}
      {durable.isError && (
        <p role="alert" className="text-sm text-status-failed">
          Activity could not be loaded.{" "}
          <button
            type="button"
            className="min-h-9 underline"
            onClick={() => void durable.refetch()}
          >
            Retry
          </button>
        </p>
      )}
      {!durable.isLoading &&
        !durable.isError &&
        !visibleItems.length &&
        !hasSettledResponse(detail) && (
          <p className="py-6 text-sm text-foreground-muted">
            {durable.gaps.length > 0
              ? "No messages, thoughts, or tool calls appear in this loaded window. Load earlier activity below."
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
      <ActivityHistory
        headerActions={viewOptions}
        conclusion={
          hasSettledResponse(detail) ? (
            <RunFinalResponse
              key={detail.attempt_id}
              detail={detail}
              disclosure={responseDisclosure}
              onDisclosureChange={setResponseDisclosure}
              repeated={repeatsSettledResponse(detail, state.items)}
            />
          ) : undefined
        }
        scopeKey={JSON.stringify([runId, attemptId])}
        items={visibleItems}
        gaps={durable.gaps}
        isLoadingMore={durable.isLoadingMore}
        loadError={durable.loadError}
        loadMore={durable.loadMore}
      />
    </section>
  );
}
