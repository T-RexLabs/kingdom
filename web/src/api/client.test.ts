import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, apiFetch } from "./client";

describe("apiFetch", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("defaults API requests to cache no-store", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ status: "ok" }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await apiFetch("/health");

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/health",
      expect.objectContaining({
        cache: "no-store",
      }),
    );
  });

  it("preserves an explicit cache mode when a caller provides one", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ status: "ok" }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await apiFetch("/health", { cache: "reload" });

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/health",
      expect.objectContaining({
        cache: "reload",
      }),
    );
  });

  it("preserves canonical API error metadata for callers", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: false,
      status: 503,
      statusText: "Service Unavailable",
      json: async () => ({
        error: {
          code: "internal_error",
          message: "provider model catalog unavailable",
          retryable: true,
          category: "core",
          details: { reason: "catalog timeout" },
          correlation_id: "api.create_run",
          occurred_at: "2026-05-21T11:00:00Z",
        },
      }),
    }));

    await expect(apiFetch("/runs")).rejects.toEqual(expect.objectContaining({
      constructor: ApiError,
      status: 503,
      code: "internal_error",
      retryable: true,
      category: "core",
      details: { reason: "catalog timeout" },
      correlationId: "api.create_run",
      occurredAt: "2026-05-21T11:00:00Z",
      message: "provider model catalog unavailable",
    }));
  });

  it("treats non-canonical 5xx HTTP errors as ambiguous by default", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: false,
      status: 502,
      statusText: "Bad Gateway",
      json: async () => {
        throw new Error("not json");
      },
    }));

    await expect(apiFetch("/runs")).rejects.toEqual(expect.objectContaining({
      constructor: ApiError,
      status: 502,
      code: "unknown",
      retryable: true,
      correlationId: "api",
      message: "Bad Gateway",
    }));
  });

  it.each([
    { status: 400, statusText: "Bad Request" },
    { status: 401, statusText: "Unauthorized" },
    { status: 413, statusText: "Payload Too Large" },
  ])("treats non-canonical $status HTTP errors as definitive client rejections", async ({ status, statusText }) => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: false,
      status,
      statusText,
      json: async () => {
        throw new Error("not json");
      },
    }));

    await expect(apiFetch("/runs")).rejects.toEqual(expect.objectContaining({
      constructor: ApiError,
      status,
      code: "unknown",
      retryable: false,
      correlationId: "api",
      message: statusText,
    }));
  });
});
