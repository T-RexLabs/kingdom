import { CircleAlert, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { ProviderModelCatalogState } from "./use-provider-model-catalog";

/**
 * Why the operator cannot choose a model, and how to try again.
 *
 * There is one treatment because there is one reason to interrupt anyone: the
 * selector beside this notice has nothing usable in it. A catalog Kingdom
 * refreshed unsuccessfully but can still serve from cache carries no message at
 * all, so this renders nothing.
 */
export function ProviderModelCatalogNotice({
  state,
  onRetry,
  retryPending = false,
  className = "",
}: {
  state: ProviderModelCatalogState;
  onRetry?: () => void;
  retryPending?: boolean;
  className?: string;
}) {
  if (!state.message) {
    return null;
  }
  return (
    <div
      className={`flex flex-wrap items-center gap-2 rounded-md border border-status-failed/30 bg-status-failed/10 px-3 py-2 text-xs text-status-failed ${className}`.trim()}
    >
      <CircleAlert className="h-4 w-4 shrink-0" />
      <div className="flex min-w-0 flex-1 items-center gap-2">
        <span>{state.message}</span>
        {onRetry ? (
          <Button type="button" variant="outline" size="sm" disabled={retryPending} className="h-7 border-status-failed/30 bg-transparent text-status-failed hover:bg-status-failed/10 disabled:cursor-not-allowed disabled:opacity-60" onClick={onRetry}>
            <RefreshCw className="h-3.5 w-3.5" />
            {retryPending ? "Refreshing..." : "Retry"}
          </Button>
        ) : null}
      </div>
    </div>
  );
}
