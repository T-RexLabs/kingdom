import { useId, useMemo, useState } from "react";
import { ChevronDown } from "lucide-react";
import {
  ActivityHistory,
  type ActivityHistoryControls,
} from "./activity-history";
import type { ActivityItem } from "./activity-view";
import { ActivityPreview } from "./activity-preview";

type ActivityDisclosureProps = ActivityHistoryControls & {
  scopeKey: string;
  items: ActivityItem[];
  isLive: boolean;
  hasFinalResponse: boolean;
  completionBehavior: "collapse" | "keep-open";
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
  hasFinalResponse,
  completionBehavior,
  ...history
}: ActivityDisclosureProps) {
  const [open, setOpen] = useState(false);
  const [hasOpened, setHasOpened] = useState(false);
  const showPreview = completionBehavior === "keep-open" || !hasFinalResponse;
  const openHistory = () => {
    setHasOpened(true);
    setOpen(true);
  };
  const panelId = useId();
  const visibleItems = useMemo(
    () => history.items.filter((item) => item.kind !== "progress"),
    [history.items],
  );
  const hasHistory = visibleItems.length > 0 || history.gaps.length > 0;
  const readError = error ?? (!hasHistory ? history.loadError : null);
  const showHistoryControl = hasHistory || isLoading || Boolean(readError);
  if (!hasHistory && !isLive && !isLoading && !readError) return null;

  return (
    <div className="space-y-2">
      {showPreview && (
        <ActivityPreview
          items={history.items}
          isLive={isLive}
          hasGaps={history.gaps.length > 0}
          onReadMore={openHistory}
        />
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
                    No thinking, tool calls, or messages in the loaded activity.
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
