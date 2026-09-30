import { ActivityHistory } from "@/features/activity/activity-history";
import { useMemo, useState } from "react";
import type { PlannerMessageActivityState } from "./use-planner-message-activity";

/** The persisted message tree owns the reply; provider messages are optional inspection. */
export function PlannerMessageActivity({
  scopeKey,
  activity,
}: {
  scopeKey: string;
  activity: PlannerMessageActivityState;
}) {
  const [showProgress, setShowProgress] = useState(false);
  const [showMessages, setShowMessages] = useState(false);
  const visibleItems = useMemo(
    () =>
      activity.items.filter(
        (item) =>
          item.kind === "thinking" ||
          item.kind === "tool" ||
          (item.kind === "progress" && showProgress) ||
          (item.kind === "message" && showMessages),
      ),
    [activity.items, showProgress, showMessages],
  );
  const hasHistory = activity.items.length > 0 || activity.gaps.length > 0;
  if (!hasHistory && !activity.isLoading && !activity.error) return null;

  return (
    <section aria-label="Planner activity" className="space-y-2">
      {activity.isLoading && (
        <p className="text-xs text-foreground-muted">Loading activity…</p>
      )}
      {activity.error && (
        <p role="alert" className="text-xs text-status-failed">
          Activity could not be loaded.{" "}
          <button
            type="button"
            className="min-h-9 underline"
            onClick={() => void activity.retry()}
          >
            Retry Activity
          </button>
        </p>
      )}
      {hasHistory && (
        <ActivityHistory
          scopeKey={scopeKey}
          messageDisplay="inspection"
          items={visibleItems}
          gaps={activity.gaps}
          isLoadingMore={activity.isLoadingMore}
          loadError={activity.loadError}
          loadMore={activity.loadMore}
          headerActions={
            <details className="max-w-lg text-xs text-foreground-muted">
              <summary className="min-h-9 cursor-pointer py-2">
                View Options
              </summary>
              <div className="space-y-2 rounded-lg border border-border bg-surface p-3">
                <p>
                  {visibleItems.length} Shown · {activity.items.length} Loaded
                </p>
                <label className="flex min-h-9 items-center gap-2">
                  <input
                    type="checkbox"
                    checked={showProgress}
                    onChange={(event) => setShowProgress(event.target.checked)}
                  />
                  Routine Progress
                </label>
                <label className="flex min-h-9 items-center gap-2">
                  <input
                    type="checkbox"
                    checked={showMessages}
                    onChange={(event) => setShowMessages(event.target.checked)}
                  />
                  Provider Messages
                </label>
                <p>
                  Provider observations are available for inspection. The reply
                  below comes from the saved conversation.
                </p>
              </div>
            </details>
          }
        />
      )}
    </section>
  );
}
