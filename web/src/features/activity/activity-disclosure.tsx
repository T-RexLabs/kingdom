import { useId, useState } from "react";
import { ChevronDown } from "lucide-react";
import {
  ActivityHistory,
  type ActivityHistoryControls,
} from "./activity-history";
import type { ActivityItem } from "./activity-view";

type ActivityDisclosureProps = ActivityHistoryControls & {
  scopeKey: string;
  items: ActivityItem[];
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
  ...history
}: ActivityDisclosureProps) {
  const [open, setOpen] = useState(false);
  const [hasOpened, setHasOpened] = useState(false);
  const panelId = useId();
  const hasHistory = history.items.length > 0 || history.gaps.length > 0;
  if (!hasHistory && !isLoading && !error) return null;

  return (
    <div className="space-y-2">
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
      {error && (
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
            {hasHistory || (!isLoading && !error) ? (
              <ActivityHistory {...history} />
            ) : null}
          </>
        )}
      </div>
    </div>
  );
}
