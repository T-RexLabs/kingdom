import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { ApiError } from "./client";
import { useSlicedMutation } from "./use-sliced-mutation";

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

function pendingError() {
  return new ApiError(409, {
    message: "reservation pending",
    code: "idempotency_pending",
    retryable: true,
    category: "core",
    details: {},
    correlationId: "test",
  });
}

describe("useSlicedMutation recovery", () => {
  it("reuses the exact title request after idempotency_pending", async () => {
    const mutation = vi.fn()
      .mockRejectedValueOnce(pendingError())
      .mockResolvedValueOnce({ title: "Original" })
      .mockResolvedValueOnce({ title: "Replacement" });
    const onSuccess = vi.fn();
    const { result } = renderHook(() => useSlicedMutation(mutation, onSuccess), { wrapper });
    const original = { title: "Original", key: "title-key-1" };

    await act(async () => {
      await expect(result.current.mutateAsync(original)).rejects.toThrow("reservation pending");
      await expect(result.current.mutateAsync({ title: "Replacement", key: "title-key-2" })).resolves.toEqual({ title: "Replacement" });
    });

    expect(mutation.mock.calls[1]?.[0]).toBe(original);
    expect(mutation.mock.calls[2]?.[0]).toEqual({ title: "Replacement", key: "title-key-2" });
    expect(onSuccess).toHaveBeenCalledWith({ title: "Replacement" });
  });

  it("uses a fresh model request after a definitive API failure", async () => {
    const definitive = new ApiError(500, {
      message: "rejected",
      code: "internal_error",
      retryable: true,
      category: "core",
      details: {},
      correlationId: "test",
    });
    const mutation = vi.fn()
      .mockRejectedValueOnce(definitive)
      .mockResolvedValueOnce({ model: "replacement" });
    const { result } = renderHook(() => useSlicedMutation(mutation, vi.fn()), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync({ model: "original", key: "model-key-1" })).rejects.toBe(definitive);
      await expect(result.current.mutateAsync({ model: "replacement", key: "model-key-2" })).resolves.toEqual({ model: "replacement" });
    });

    expect(mutation.mock.calls[1]?.[0]).toEqual({ model: "replacement", key: "model-key-2" });
  });
});
