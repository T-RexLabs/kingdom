import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ApiError } from "@/api/client";
import {
  resolveProviderModelCatalogState,
  useProviderModelCatalogController,
} from "./use-provider-model-catalog";

const { useProviderModelsMock, useRefreshProviderModelsMock } = vi.hoisted(() => ({
  useProviderModelsMock: vi.fn(),
  useRefreshProviderModelsMock: vi.fn(),
}));

vi.mock("@/api/hooks", () => ({
  useProviderModels: () => useProviderModelsMock(),
  useRefreshProviderModels: () => useRefreshProviderModelsMock(),
}));

function makeApiError(message: string) {
  return new ApiError(503, {
    message,
    code: "internal_error",
    retryable: true,
    category: "core",
    details: {},
    correlationId: "api.test",
  });
}

const cachedModel = {
  id: "github-copilot/gpt-5.4",
  provider_id: "github-copilot",
  provider_name: "GitHub Copilot",
  model_name: "GPT-5.4",
  thinking_effort_options: [{ value: "medium", label: "Medium" }],
};

describe("resolveProviderModelCatalogState", () => {
  it("says nothing when a background refresh fails but cached models remain", () => {
    const state = resolveProviderModelCatalogState(
      { catalog_status: "fresh", items: [cachedModel] },
      makeApiError("provider model catalog refresh failed"),
      false,
    );

    // Nothing was asked for and nothing was lost: the selector still works and
    // the poll behind it recovers on its own. Reporting the failed chore would
    // interrupt the operator over Kingdom's problem, not theirs.
    expect(state.items).toHaveLength(1);
    expect(state.disableSelection).toBe(false);
    expect(state.message).toBeNull();
  });

  it("ignores the server's stale copy while models are still selectable", () => {
    const state = resolveProviderModelCatalogState(
      {
        catalog_status: "stale",
        items: [cachedModel],
        failure_reason: "command_failed",
        failure_message: "Showing the models Kingdom already had.",
      },
      undefined,
      false,
    );

    expect(state.disableSelection).toBe(false);
    expect(state.message).toBeNull();
  });

  it("explains an unreachable catalog and blocks selection", () => {
    const state = resolveProviderModelCatalogState(
      { catalog_status: "fresh", items: [] },
      makeApiError("provider model catalog refresh failed"),
      false,
    );

    expect(state.items).toHaveLength(0);
    expect(state.disableSelection).toBe(true);
    expect(state.message).toContain("refresh failed");
  });

  it("explains an account with no models at all", () => {
    const state = resolveProviderModelCatalogState(
      { catalog_status: "fresh", items: [] },
      undefined,
      false,
    );

    expect(state.disableSelection).toBe(true);
    expect(state.message).toBe("No models are available for this account.");
  });
});

describe("useProviderModelCatalogController", () => {
  it("keeps a forced refresh failure visible when an overlapping background query succeeds", () => {
    // Retry is only reachable from a blocked catalog, so that is the state this
    // guards: the operator asked a direct question, and a background poll
    // resolving underneath them must not overwrite the answer with generic copy.
    const emptyResponse = { catalog_status: "fresh" as const, items: [] };
    let query = {
      data: emptyResponse,
      dataUpdatedAt: 100,
      error: null,
      isLoading: false,
    };
    let refreshCallbacks:
      | { onError: (error: unknown) => void; onSuccess: () => void }
      | undefined;
    const mutate = vi.fn(
      (
        _variables: undefined,
        callbacks: { onError: (error: unknown) => void; onSuccess: () => void },
      ) => {
        refreshCallbacks = callbacks;
      },
    );
    useProviderModelsMock.mockImplementation(() => query);
    useRefreshProviderModelsMock.mockReturnValue({ mutate, isPending: false });

    const hook = renderHook(() => useProviderModelCatalogController());

    act(() => hook.result.current.retry());
    act(() => refreshCallbacks?.onError(makeApiError("forced refresh failed")));
    expect(hook.result.current.state.message).toBe("forced refresh failed");

    query = { ...query, data: { ...emptyResponse }, dataUpdatedAt: 200 };
    hook.rerender();

    expect(hook.result.current.state.disableSelection).toBe(true);
    expect(hook.result.current.state.message).toBe("forced refresh failed");
  });

  it("clears the notice once a retry restores the catalog", () => {
    const query = {
      data: { catalog_status: "fresh" as const, items: [cachedModel] },
      dataUpdatedAt: 100,
      error: null,
      isLoading: false,
    };
    let refreshCallbacks:
      | { onError: (error: unknown) => void; onSuccess: () => void }
      | undefined;
    const mutate = vi.fn(
      (
        _variables: undefined,
        callbacks: { onError: (error: unknown) => void; onSuccess: () => void },
      ) => {
        refreshCallbacks = callbacks;
      },
    );
    useProviderModelsMock.mockImplementation(() => query);
    useRefreshProviderModelsMock.mockReturnValue({ mutate, isPending: false });

    const hook = renderHook(() => useProviderModelCatalogController());

    act(() => hook.result.current.retry());
    act(() => refreshCallbacks?.onError(makeApiError("forced refresh failed")));
    expect(hook.result.current.state.message).toBeNull();

    act(() => refreshCallbacks?.onSuccess());
    expect(hook.result.current.state.message).toBeNull();
    expect(hook.result.current.state.disableSelection).toBe(false);
  });
});
