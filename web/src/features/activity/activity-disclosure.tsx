import { useId, useMemo, useState } from "react";
import { ChevronDown } from "lucide-react";
import {
  ActivityHistory,
  type ActivityHistoryControls,
} from "./activity-history";
import { activityStatusLine } from "./activity-status";
import { isConversationActivity, type ActivityItem } from "./activity-view";

type ActivityDisclosureProps = ActivityHistoryControls & {
  scopeKey: string;
  items: ActivityItem[];
  isLive: boolean;
  isLoading?: boolean;
  error?: Error | null;
  retry?: () => Promise<unknown>;
};

export function ActivityDisclosure(props: ActivityDisclosureProps) {
  return <ScopedActivityDisclosure key={props.scopeKey} {...props} />;
}

function ScopedActivityDisclosure({
  isLoading,
  error,
  retry,
  isLive,
  ...history
}: ActivityDisclosureProps) {
  const [open, setOpen] = useState(false);
  const [hasOpened, setHasOpened] = useState(false);
  const panelId = useId();
  const visibleItems = useMemo(
    () => history.items.filter(isConversationActivity),
    [history.items],
  );
  const hasHistory = visibleItems.length > 0 || history.gaps.length > 0;
  const readError = error ?? (!hasHistory ? history.loadError : null);
  const showHistoryControl = hasHistory || isLoading || Boolean(readError);
  if (!hasHistory && !isLive && !isLoading && !readError) return null;

  return (
    <div className="space-y-2">
      <div className="flex min-w-0 flex-wrap items-center gap-x-4 gap-y-1">
        {isLive && (
          <p
            role="status"
            className="min-w-0 truncate text-xs text-foreground-muted"
          >
            {activityStatusLine(history.items)}
          </p>
        )}
        {showHistoryControl && (
          <button
            type="button"
            aria-expanded={open}
            aria-controls={panelId}
            onClick={() => {
              setHasOpened(true);
              setOpen((current) => !current);
            }}
            className="inline-flex min-h-9 items-center gap-2 text-xs font-medium text-foreground-muted transition-colors hover:text-foreground"
          >
            <ChevronDown
              aria-hidden="true"
              className={`h-3.5 w-3.5 transition-transform ${open ? "rotate-180" : ""}`}
            />
            {open ? "Hide Activity" : "Show Activity"}
          </button>
        )}
      </div>
      {readError && (
        <p role="alert" className="text-xs text-status-failed">
          Activity could not be loaded.{" "}
          {retry && (
            <button
              type="button"
              className="min-h-9 underline"
              onClick={() => void retry()}
            >
              Retry Activity
            </button>
          )}
        </p>
      )}
      {showHistoryControl && (
        <div
          id={panelId}
          hidden={!open}
          className="rounded-xl border border-border/60 bg-surface/40 px-3 py-2"
        >
          {hasOpened && (
            <>
              {isLoading && (
                <p className="py-2 text-xs text-foreground-muted">
                  Loading activity…
                </p>
              )}
              {!visibleItems.length &&
                history.gaps.length > 0 &&
                !isLoading &&
                !readError && (
                  <p className="py-2 text-xs text-foreground-muted">
                    No thinking or tool calls in the loaded activity.
                  </p>
                )}
              {hasHistory ? (
                <ActivityHistory {...history} items={visibleItems} />
              ) : null}
            </>
          )}
        </div>
      )}
    </div>
  );
}
