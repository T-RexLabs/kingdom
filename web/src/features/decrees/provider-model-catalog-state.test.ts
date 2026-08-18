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

describe("resolveProviderModelCatalogState", () => {
  it("keeps cached models selectable when a refetch error arrives", () => {
    const state = resolveProviderModelCatalogState(
      {
        catalog_status: "fresh",
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [{ value: "medium", label: "Medium" }],
          },
        ],
      },
      makeApiError("provider model catalog refresh failed"),
      false,
    );

    expect(state.status).toBe("stale");
    expect(state.items).toHaveLength(1);
    expect(state.disableSelection).toBe(false);
    expect(state.message).toContain("refresh failed");
  });

  it("stays unavailable when a refetch error arrives without cached models", () => {
    const state = resolveProviderModelCatalogState(
      {
        catalog_status: "fresh",
        items: [],
      },
      makeApiError("provider model catalog refresh failed"),
      false,
    );

    expect(state.status).toBe("unavailable");
    expect(state.items).toHaveLength(0);
    expect(state.disableSelection).toBe(true);
  });

  it("prefers explicit retry errors over stale response copy", () => {
    const state = resolveProviderModelCatalogState(
      {
        catalog_status: "stale",
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [{ value: "medium", label: "Medium" }],
          },
        ],
        failure_reason: "command_failed",
        failure_message: "showing cached models",
      },
      makeApiError("live refresh failed"),
      false,
    );

    expect(state.status).toBe("stale");
    expect(state.message).toBe("live refresh failed");
    expect(state.disableSelection).toBe(false);
  });
});

describe("useProviderModelCatalogController", () => {
  it("keeps a forced refresh failure visible when an overlapping background query succeeds", () => {
    const cachedResponse = {
      catalog_status: "fresh" as const,
      items: [
        {
          id: "github-copilot/gpt-5.4",
          provider_id: "github-copilot",
          provider_name: "GitHub Copilot",
          model_name: "GPT-5.4",
          thinking_effort_options: [{ value: "medium", label: "Medium" }],
        },
      ],
    };
    let query = {
      data: cachedResponse,
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

    query = {
      ...query,
      data: { ...cachedResponse },
      dataUpdatedAt: 200,
    };
    hook.rerender();

    expect(hook.result.current.state.status).toBe("stale");
    expect(hook.result.current.state.message).toBe("forced refresh failed");
  });
});
