import { runtimeMode } from "@/lib/runtime-mode";
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
  message: string | null;
  disableSelection: boolean;
  triggerPlaceholder: string;
  emptyStateLabel: string;
};

/**
 * What the model selector should show, and whether it should say anything.
 *
 * `message` is the notice, and it is set only when the operator cannot choose a
 * model. A catalog Kingdom failed to refresh is not that: the cached list stays
 * selectable, every downstream action still works, and the poll behind the
 * selector recovers on its own within a minute. Announcing that would report a
 * background chore to someone who lost nothing by it — freshness is Kingdom's
 * problem, being blocked is the operator's.
 *
 * A stale catalog therefore resolves exactly like a fresh one. The server sends
 * `stale` only alongside cached rows, and the error handling below downgrades an
 * empty list to `unavailable`, so a stale catalog always has models to offer.
 */
export function resolveProviderModelCatalogState(
  response: ProviderModelsResponse | undefined,
  error: unknown,
  isLoading: boolean,
): ProviderModelCatalogState {
  if (isLoading && response === undefined && !error) {
    return {
      isLoading: true,
      items: [],
      message: null,
      disableSelection: true,
      triggerPlaceholder: "Loading models...",
      emptyStateLabel: "Loading models...",
    };
  }

  if (response === undefined && error) {
    return {
      isLoading: false,
      items: [],
      message: providerModelCatalogErrorMessage(error),
      disableSelection: true,
      triggerPlaceholder: "Model catalog unavailable",
      emptyStateLabel: "Model catalog unavailable.",
    };
  }

  if (error && runtimeMode() === "hosted-project") {
    return { isLoading: false, items: [], message: "The model catalog is unavailable. Refresh the connection before selecting a model.", disableSelection: true, triggerPlaceholder: "Model catalog unavailable", emptyStateLabel: "Model catalog unavailable." };
  }
  const items = response?.items ?? [];
  let status = response?.catalog_status ?? "fresh";
  let failureMessage = response?.failure_message ?? null;
  if (error) {
    failureMessage = providerModelCatalogErrorMessage(error);
    status = items.length > 0 ? "stale" : "unavailable";
  }
  if (status === "unavailable") {
    return {
      isLoading: false,
      items,
      message: failureMessage ?? "Model catalog unavailable. Retry after the runtime recovers.",
      disableSelection: true,
      triggerPlaceholder: "Model catalog unavailable",
      emptyStateLabel: "Model catalog unavailable.",
    };
  }
  if (items.length === 0) {
    return {
      isLoading: false,
      items,
      message: "No models are available for this account.",
      disableSelection: true,
      triggerPlaceholder: "No models available",
      emptyStateLabel: "No models available for this account.",
    };
  }
  return {
    isLoading: false,
    items,
    message: null,
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
