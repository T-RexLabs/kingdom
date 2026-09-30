import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, apiFetch, currentAuthBinding, currentAuthGeneration, resolveAPIBase, setCSRFToken, shouldRetryApiMutation, shouldRetryApiQuery, signalUnauthorized, subscribeToUnauthorized } from "./client";

describe("resolveAPIBase", () => {
  it("uses the API base path injected by the runtime", () => {
    const documentRoot = document.implementation.createHTMLDocument();
    const metadata = documentRoot.createElement("meta");
    metadata.name = "kingdom-api-base";
    metadata.content = "/internal/api";
    documentRoot.head.append(metadata);

    expect(resolveAPIBase(documentRoot)).toBe("/internal/api");
  });
});

describe("shouldRetryApiMutation", () => {
  const canonicalError = (code: "idempotency_pending" | "idempotency_key_expired", retryable: boolean) => new ApiError(409, {
    message: code,
    code,
    retryable,
    category: "control_plane",
    details: {},
    correlationId: "test",
  });

  it("retries ambiguous failures with the same mutation variables", () => {
    expect(shouldRetryApiMutation(0, new TypeError("network"), 2)).toBe(true);
    expect(shouldRetryApiMutation(1, canonicalError("idempotency_pending", true), 2)).toBe(true);
    expect(shouldRetryApiMutation(0, canonicalError("idempotency_key_expired", false), 2)).toBe(false);
    expect(shouldRetryApiMutation(2, new TypeError("network"), 2)).toBe(false);
  });
});

describe("apiFetch", () => {
  it("tracks the nonsecret binding and clears it when authentication is lost", () => {
    setCSRFToken("secret-csrf", "binding-one");
    expect(currentAuthBinding()).toBe("binding-one");
    signalUnauthorized();
    expect(currentAuthBinding()).toBe("");
    setCSRFToken("new-secret", "binding-two");
    expect(currentAuthBinding()).toBe("binding-two");
  });
  afterEach(() => {
	setCSRFToken("");
    vi.unstubAllGlobals();
  });

  it("uses cookie auth and adds the in-memory CSRF token only to mutations", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 204, json: async () => ({}) });
    vi.stubGlobal("fetch", fetchMock);
    setCSRFToken("csrf-test");

    await apiFetch("/runs/run-1/cancel", { method: "POST" });

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/runs/run-1/cancel",
      expect.objectContaining({
        credentials: "same-origin",
        headers: expect.objectContaining({ "X-CSRF-Token": "csrf-test" }),
      }),
    );
    const headers = fetchMock.mock.calls[0]?.[1]?.headers as Record<string, string>;
    expect(headers["X-Actor-Id"]).toBeUndefined();
    expect(headers["X-Role-Id"]).toBeUndefined();
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
          retry_at: "2026-05-21T11:05:00Z",
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
      retryAt: "2026-05-21T11:05:00Z",
      message: "provider model catalog unavailable",
    }));
  });

  it("discards malformed retry timestamps", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: false,
      status: 429,
      statusText: "Too Many Requests",
      json: async () => ({ error: { code: "dependency_blocked", message: "rate limited", retryable: false, category: "core", details: {}, correlation_id: "api.test", occurred_at: "2026-05-21T11:00:00Z", retry_at: "provider said later" } }),
    }));

    await expect(apiFetch("/connections")).rejects.toEqual(expect.objectContaining({
      constructor: ApiError,
      retryAt: undefined,
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

  it("coalesces protected-request 401 signals until a new session is established", async () => {
    const listener = vi.fn();
    const unsubscribe = subscribeToUnauthorized(listener);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 401, statusText: "Unauthorized" })));

    await expect(apiFetch("/runs")).rejects.toBeInstanceOf(ApiError);
    await expect(apiFetch("/roles")).rejects.toBeInstanceOf(ApiError);
    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener).toHaveBeenCalledWith(expect.objectContaining({ source: "read" }));
    expect(shouldRetryApiQuery(0, new ApiError(401, {
      message: "Unauthorized", code: "unknown", retryable: false, category: "core", details: {}, correlationId: "api",
    }), 3)).toBe(false);

    setCSRFToken("new-session");
    await expect(apiFetch("/runs")).rejects.toBeInstanceOf(ApiError);
    expect(listener).toHaveBeenCalledTimes(2);
    unsubscribe();
  });

  it("recovers the session after a canonical CSRF rejection without replaying the mutation", async () => {
    const listener = vi.fn();
    const unsubscribe = subscribeToUnauthorized(listener);
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      error: {
        code: "policy_violation",
        message: "same-origin CSRF validation failed",
        retryable: false,
        category: "core",
        details: {},
        correlation_id: "api.auth.csrf",
        occurred_at: "2026-08-17T12:00:00Z",
      },
    }), { status: 403, statusText: "Forbidden", headers: { "Content-Type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(apiFetch("/runs/run-1/cancel", { method: "POST" })).rejects.toBeInstanceOf(ApiError);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(listener).toHaveBeenCalledWith(expect.objectContaining({ source: "mutation" }));
    unsubscribe();
  });

  it("does not signal local sign-out for revoked GitHub dependency failures", async () => {
    const listener = vi.fn();
    const unsubscribe = subscribeToUnauthorized(listener);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({
      error: {
        code: "dependency_blocked",
        message: "GitHub authorization is invalid or revoked; reconnect the connection before retrying",
        retryable: false,
        category: "core",
        details: { provider_failure_class: "revoked" },
        correlation_id: "api.connection",
        occurred_at: "2026-08-15T12:00:00Z",
      },
    }), { status: 424, statusText: "Failed Dependency", headers: { "Content-Type": "application/json" } })));

    await expect(apiFetch("/connections/connection-1/repositories")).rejects.toEqual(expect.objectContaining({
      status: 424,
      code: "dependency_blocked",
      details: { provider_failure_class: "revoked" },
    }));
    expect(listener).not.toHaveBeenCalled();
    unsubscribe();
  });

  it("ignores late unauthorized responses from an older browser session", () => {
    const listener = vi.fn();
    const unsubscribe = subscribeToUnauthorized(listener);
    const oldGeneration = currentAuthGeneration();
    setCSRFToken("replacement-session");
    signalUnauthorized("read", oldGeneration);

    expect(listener).not.toHaveBeenCalled();
    unsubscribe();
  });

  it("identifies unauthorized mutations without replaying them", async () => {
    const listener = vi.fn();
    const unsubscribe = subscribeToUnauthorized(listener);
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 401, statusText: "Unauthorized" }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(apiFetch("/runs/run-1/cancel", { method: "POST" })).rejects.toBeInstanceOf(ApiError);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(listener).toHaveBeenCalledWith(expect.objectContaining({ source: "mutation" }));
    unsubscribe();
  });
});
