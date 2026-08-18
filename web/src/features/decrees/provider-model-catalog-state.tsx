import { CircleAlert, RefreshCw, TriangleAlert } from "lucide-react";
import { type ReactNode } from "react";
import { Button } from "@/components/ui/button";
import type { ProviderModelCatalogState } from "./use-provider-model-catalog";

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
  if (state.status === "stale") {
    return (
      <CatalogNotice
        icon={<TriangleAlert className="h-4 w-4 shrink-0" />}
        className={`border-amber-500/30 bg-amber-500/10 text-amber-200 ${className}`.trim()}
      >
        <span>{state.message}</span>
        {onRetry ? (
          <Button type="button" variant="outline" size="sm" disabled={retryPending} className="h-7 border-amber-400/30 bg-transparent text-amber-100 hover:bg-amber-400/10 disabled:cursor-not-allowed disabled:opacity-60" onClick={onRetry}>
            <RefreshCw className="h-3.5 w-3.5" />
            {retryPending ? "Refreshing..." : "Retry refresh"}
          </Button>
        ) : null}
      </CatalogNotice>
    );
  }
  return (
    <CatalogNotice
      icon={<CircleAlert className="h-4 w-4 shrink-0" />}
      className={`border-status-failed/30 bg-status-failed/10 text-status-failed ${className}`.trim()}
    >
      <span>{state.message}</span>
      {onRetry ? (
        <Button type="button" variant="outline" size="sm" disabled={retryPending} className="h-7 border-status-failed/30 bg-transparent text-status-failed hover:bg-status-failed/10 disabled:cursor-not-allowed disabled:opacity-60" onClick={onRetry}>
          <RefreshCw className="h-3.5 w-3.5" />
          {retryPending ? "Refreshing..." : "Retry"}
        </Button>
      ) : null}
    </CatalogNotice>
  );
}

function CatalogNotice({
  icon,
  children,
  className,
}: {
  icon: ReactNode;
  children: ReactNode;
  className: string;
}) {
  return (
    <div className={`flex flex-wrap items-center gap-2 rounded-md border px-3 py-2 text-xs ${className}`.trim()}>
      {icon}
      <div className="flex min-w-0 flex-1 items-center gap-2">{children}</div>
    </div>
  );
}
