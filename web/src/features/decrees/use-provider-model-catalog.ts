import { useCallback, useMemo, useState } from "react";
import { ApiError } from "@/api/client";
import {
  useProviderModels,
  useRefreshProviderModels,
  type ProviderModel,
  type ProviderModelsResponse,
} from "@/api/hooks";

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
    () =>
      resolveProviderModelCatalogState(
        providerModelsQuery.data,
        refreshError ?? providerModelsQuery.error,
        providerModelsQuery.isLoading,
      ),
    [providerModelsQuery.data, providerModelsQuery.error, providerModelsQuery.isLoading, refreshError],
  );

  return {
    state,
    retry,
    retryPending: refreshProviderModels.isPending,
    query: providerModelsQuery,
  };
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
