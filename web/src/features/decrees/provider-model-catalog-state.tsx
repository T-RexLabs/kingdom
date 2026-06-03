import { CircleAlert, RefreshCw, TriangleAlert } from "lucide-react";
import { type ReactNode, useCallback, useEffect, useMemo, useState } from "react";
import { ApiError } from "@/api/client";
import { useProviderModels, useRefreshProviderModels, type ProviderModel, type ProviderModelsResponse } from "@/api/hooks";
import { Button } from "@/components/ui/button";

export type ProviderModelCatalogState = {
  isLoading: boolean;
  items: ProviderModel[];
  status: "fresh" | "stale" | "unavailable";
  message: string | null;
  failureReason: string | null;
  disableSelection: boolean;
  triggerPlaceholder: string;
  emptyStateLabel: string;
};

export function resolveProviderModelCatalogState(
  response: ProviderModelsResponse | undefined,
  error: unknown,
  isLoading: boolean,
): ProviderModelCatalogState {
  if (isLoading && response === undefined && !error) {
    return {
      isLoading: true,
      items: [],
      status: "fresh",
      message: null,
      failureReason: null,
      disableSelection: true,
      triggerPlaceholder: "Loading models...",
      emptyStateLabel: "Loading models...",
    };
  }

  if (response === undefined && error) {
    return {
      isLoading: false,
      items: [],
      status: "unavailable",
      message: providerModelCatalogErrorMessage(error),
      failureReason: null,
      disableSelection: true,
      triggerPlaceholder: "Model catalog unavailable",
      emptyStateLabel: "Model catalog unavailable.",
    };
  }

  const items = response?.items ?? [];
  let status = response?.catalog_status ?? "fresh";
  const failureReason = response?.failure_reason ?? null;
  let failureMessage = response?.failure_message ?? null;
	if (error) {
		failureMessage = providerModelCatalogErrorMessage(error);
		if (items.length > 0) {
			if (status === "fresh") {
				status = "stale";
			}
		} else {
			status = "unavailable";
		}
	}
  if (status === "unavailable") {
    return {
      isLoading: false,
      items,
      status,
      message: failureMessage ?? "Model catalog unavailable. Retry after the runtime recovers.",
      failureReason,
      disableSelection: true,
      triggerPlaceholder: "Model catalog unavailable",
      emptyStateLabel: "Model catalog unavailable.",
    };
  }
  if (status === "stale") {
    return {
      isLoading: false,
      items,
      status,
      message: failureMessage ?? "Showing cached models while the live provider catalog refresh is unavailable.",
      failureReason,
      disableSelection: items.length === 0,
      triggerPlaceholder: items.length === 0 ? "No cached models available" : "Select model",
      emptyStateLabel: items.length === 0 ? "No cached models available." : "No models found.",
    };
  }
  if (items.length === 0) {
    return {
      isLoading: false,
      items,
      status,
      message: "No models are available for this account.",
      failureReason,
      disableSelection: true,
      triggerPlaceholder: "No models available",
      emptyStateLabel: "No models available for this account.",
    };
  }
  return {
    isLoading: false,
    items,
    status,
    message: null,
    failureReason,
    disableSelection: false,
    triggerPlaceholder: "Select model",
    emptyStateLabel: "No models found.",
  };
}

export function useProviderModelCatalogController() {
	const providerModelsQuery = useProviderModels();
	const refreshProviderModels = useRefreshProviderModels();
	const [refreshError, setRefreshError] = useState<unknown>(null);

	useEffect(() => {
		if (providerModelsQuery.error == null && providerModelsQuery.data !== undefined) {
			setRefreshError(null);
		}
	}, [providerModelsQuery.data, providerModelsQuery.dataUpdatedAt, providerModelsQuery.error]);

	const retry = useCallback(() => {
		setRefreshError(null);
		refreshProviderModels.mutate(undefined, {
			onError: (error) => {
				setRefreshError(error);
			},
			onSuccess: () => {
				setRefreshError(null);
			},
		});
	}, [refreshProviderModels]);

	const state = useMemo(
		() => resolveProviderModelCatalogState(providerModelsQuery.data, refreshError ?? providerModelsQuery.error, providerModelsQuery.isLoading),
		[providerModelsQuery.data, providerModelsQuery.error, providerModelsQuery.isLoading, refreshError],
	);

	return {
		state,
		retry,
		retryPending: refreshProviderModels.isPending,
		query: providerModelsQuery,
	};
}

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

function providerModelCatalogErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error && error.message.trim() !== "") {
    return error.message;
  }
  return "Model catalog unavailable. Retry after the runtime recovers.";
}
