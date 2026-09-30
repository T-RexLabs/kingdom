import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ApiError } from "@/api/client";
import { useRecoverableMutation } from "./use-recoverable-mutation";

function apiError(code: "idempotency_pending" | "internal_error" | "policy_violation", retryable: boolean) {
  const status = code === "idempotency_pending" ? 409 : code === "internal_error" ? 500 : 400;
  return new ApiError(status, {
    message: code,
    code,
    retryable,
    category: "core",
    details: {},
    correlationId: "test",
  });
}

describe("useRecoverableMutation", () => {
  it.each([
    ["transport failure", new TypeError("connection lost")],
    ["pending reservation", apiError("idempotency_pending", true)],
  ])("reuses the exact request and key after %s", async (_name, failure) => {
    const mutation = vi.fn()
      .mockRejectedValueOnce(failure)
      .mockResolvedValueOnce("recovered");
    const { result } = renderHook(() => useRecoverableMutation("test-mutation", mutation));
    const original = { action: "first", body: { value: 1 } };

    await act(async () => {
      await expect(result.current.mutateAsync(original)).rejects.toBe(failure);
    });
    expect(result.current.pendingRequest).toBe(original);

    await act(async () => {
      await expect(result.current.mutateAsync({ action: "replacement", body: { value: 2 } })).resolves.toBe("recovered");
    });

    expect(mutation.mock.calls[1]?.[0]).toBe(original);
    expect(mutation.mock.calls[1]?.[1]).toBe(mutation.mock.calls[0]?.[1]);
    expect(result.current.hasPendingRecovery).toBe(false);
  });

  it("clears a retained request after a definitive API error", async () => {
    const mutation = vi.fn()
      .mockRejectedValueOnce(new TypeError("connection lost"))
      .mockRejectedValueOnce(apiError("policy_violation", false))
      .mockResolvedValueOnce("fresh");
    const { result } = renderHook(() => useRecoverableMutation("test-mutation", mutation));

    await act(async () => {
      await expect(result.current.mutateAsync({ value: 1 })).rejects.toThrow("connection lost");
      await expect(result.current.retry()).rejects.toThrow("policy_violation");
    });
    expect(result.current.hasPendingRecovery).toBe(false);

    await act(async () => {
      await expect(result.current.mutateAsync({ value: 2 })).resolves.toBe("fresh");
    });
    expect(mutation.mock.calls[2]?.[0]).toEqual({ value: 2 });
    expect(mutation.mock.calls[2]?.[1]).not.toBe(mutation.mock.calls[1]?.[1]);
  });

  it("does not replay a retained request after resource identity changes", async () => {
    const mutation = vi.fn()
      .mockRejectedValueOnce(apiError("idempotency_pending", true))
      .mockResolvedValueOnce("fresh-resource");
    const { result, rerender } = renderHook(
      ({ identity }) => useRecoverableMutation("test-mutation", mutation, identity),
      { initialProps: { identity: "resource-a" } },
    );

    await act(async () => {
      await expect(result.current.mutateAsync({ value: "a" })).rejects.toThrow("idempotency_pending");
    });
    const firstKey = mutation.mock.calls[0]?.[1];
    rerender({ identity: "resource-b" });
    expect(result.current.hasPendingRecovery).toBe(false);
    await act(async () => {
      await expect(result.current.mutateAsync({ value: "b" })).resolves.toBe("fresh-resource");
    });
    expect(mutation.mock.calls[1]?.[0]).toEqual({ value: "b" });
    expect(mutation.mock.calls[1]?.[1]).not.toBe(firstKey);
  });
});
