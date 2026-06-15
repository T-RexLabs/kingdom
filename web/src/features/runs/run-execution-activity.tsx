import { useMemo, useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useRunExecutionActivity } from "@/api/hooks-runs";
import { activityItemFromApi, buildActivityState } from "@/features/activity/activity-view";
import { ActivityLiveStream, ActivitySources, ActivityThoughts } from "@/features/activity/activity-stream";
import { selectLiveRunActivity, useLiveRunActivity } from "./run-live-activity";

// RunExecutionActivity surfaces the fine-grained agent activity (reasoning + tool
// calls) for the selected node attempt: live while the attempt is running, then a
// settled Thoughts / Sources trail. The whole section is foldable and collapsed by
// default — the header shows the current action so the operator knows work is
// happening, and they expand to read it. It renders nothing for attempts that
// produced no activity (e.g. runtime-owned nodes), so it never leaves an empty shell.
export function RunExecutionActivity({
  runId,
  attemptId,
  isRunning,
}: {
  runId: string;
  attemptId: string | null;
  isRunning: boolean;
}) {
  const [open, setOpen] = useState(false);
  const durableQuery = useRunExecutionActivity(runId, attemptId, {
    refetchIntervalMs: isRunning ? 1_500 : false,
  });
  const liveBuffer = useLiveRunActivity(runId);
  const liveItems = selectLiveRunActivity(liveBuffer, attemptId);

  const state = useMemo(
    () =>
      buildActivityState({
        durableItems: (durableQuery.data?.items ?? []).map(activityItemFromApi),
        liveItems,
      }),
    [durableQuery.data?.items, liveItems],
  );

  const showLive = isRunning && Boolean(state.currentEvent || state.liveThoughtText.trim());
  const showThoughts = !isRunning && state.thoughts.length > 0;
  const showSources = !isRunning && state.sources.length > 0;
  if (!showLive && !showThoughts && !showSources) {
    return null;
  }

  return (
    <section className="rounded-xl border border-border bg-background/70">
      <button
        type="button"
        onClick={() => setOpen((current) => !current)}
        aria-expanded={open}
        className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left"
      >
        <span className="flex min-w-0 items-center gap-2">
          <span className="text-xs uppercase tracking-[0.16em] text-foreground-muted">Activity</span>
          {/* Surface the current action in the collapsed header so live work is
              visible at a glance without expanding. The lanes card above carries the
              animated liveness cue, so no quill here. */}
          {showLive && state.currentEvent ? (
            <span className="min-w-0 truncate text-xs text-foreground-subtle">{state.currentEvent.label}</span>
          ) : null}
        </span>
        {open
          ? <ChevronDown className="h-4 w-4 shrink-0 text-foreground-subtle" />
          : <ChevronRight className="h-4 w-4 shrink-0 text-foreground-subtle" />}
      </button>
      {open ? (
        <div className="space-y-3 border-t border-border px-4 py-3">
          {showLive ? <ActivityLiveStream state={state} isStreaming={isRunning} /> : null}
          {showThoughts ? <ActivityThoughts thoughts={state.thoughts} /> : null}
          {showSources ? <ActivitySources sources={state.sources} /> : null}
        </div>
      ) : null}
    </section>
  );
}
