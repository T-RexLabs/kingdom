import { beforeEach, expect, it, vi } from "vitest";
import { ApiError, setCSRFToken } from "@/api/client";
import * as apiClient from "@/api/client";
import { newIdempotencyKey } from "@/lib/idempotency";
import { themeAPI, type SharedThemeRequest } from "./theme-api";
import { ThemeOperation, validStoredAttempt } from "./use-theme-operation";

beforeEach(() => {
  sessionStorage.clear();
  vi.restoreAllMocks();
  setCSRFToken("csrf", "first-session");
});
it("recovers an unknown save with its original request and idempotency key after navigation/reload", async () => {
  const mutate = vi
    .spyOn(themeAPI, "mutate")
    .mockRejectedValueOnce(new TypeError("network disconnected"))
    .mockResolvedValue({ saved: true });
  const original = {
    method: "PUT" as const,
    path: "/themes/custom/source",
    body: {
      source: '{"id":"custom","name":"original"}',
      expected_source_digest: "a".repeat(64),
      assets: {},
    },
  };
  const operation = new ThemeOperation("recovery");
  await expect(operation.run(original)).rejects.toThrow("network disconnected");
  original.body.source = '{"id":"custom","name":"later edit"}';
  const recovered = new ThemeOperation("recovery");
  expect(recovered.getSnapshot().pending?.request.body).toMatchObject({
    source: '{"id":"custom","name":"original"}',
  });
  await expect(recovered.run(original)).rejects.toThrow("Recover the previous");
  await recovered.retry();
  expect(mutate.mock.calls[1]).toEqual(mutate.mock.calls[0]);
  expect(recovered.getSnapshot().pending).toBeNull();
  expect(new ThemeOperation("recovery").getSnapshot().pending).toBeNull();
});
it("clears definitive conflicts so the preserved draft can be corrected and retried deliberately", async () => {
  vi.spyOn(themeAPI, "mutate").mockRejectedValue(
    new ApiError(409, {
      message: "Source changed",
      code: "dependency_blocked",
      category: "core",
      retryable: false,
      details: {},
      correlationId: "api",
    }),
  );
  const operation = new ThemeOperation("conflict");
  await expect(
    operation.run({
      method: "POST",
      path: "/themes/custom/publish",
      body: { expected_source_digest: "a".repeat(64) },
      verify: { id: "custom", resolvedDigest: "b".repeat(64) },
    }),
  ).rejects.toThrow("Source changed");
  expect(operation.getSnapshot().pending).toBeNull();
  expect(operation.getSnapshot().error).not.toBeNull();
  operation.clearError();
  expect(operation.getSnapshot().error).toBeNull();
});
it("rejects recovered requests outside the theme mutation boundary", () => {
  const key = newIdempotencyKey("test");
  const binding = {
    origin: window.location.origin,
    authBinding: "first-session",
  };
  for (const request of [
    { method: "POST", path: "/runs" },
    { method: "POST", path: "/themes/a/source" },
    { method: "DELETE", path: "/themes/../settings" },
    { method: "DELETE", path: "https://example.com/themes/a" },
  ]) {
    expect(validStoredAttempt({ key, request, ...binding })).toBe(false);
    sessionStorage.setItem(
      "kingdom:theme-operation:v1:unsafe",
      JSON.stringify({ key, request, ...binding }),
    );
    expect(new ThemeOperation("unsafe").getSnapshot().pending).toBeNull();
  }
  expect(
    validStoredAttempt({
      key,
      ...binding,
      request: {
        method: "POST",
        path: "/themes/import",
        body: { archive: "cGs=" },
        verify: { id: "custom", resolvedDigest: "b".repeat(64) },
      },
    }),
  ).toBe(true);
});
it("does not dispatch an unjournaled request when recovery storage is full", async () => {
  const storage = vi
    .spyOn(Storage.prototype, "setItem")
    .mockImplementation(() => {
      throw new Error("full");
    });
  const mutate = vi
    .spyOn(themeAPI, "mutate")
    .mockRejectedValue(new TypeError("offline"));
  const operation = new ThemeOperation("full");
  await expect(
    operation.run({ method: "DELETE", path: "/themes/custom" }),
  ).rejects.toThrow(/recovery request could not be saved/);
  expect(mutate).not.toHaveBeenCalled();
  expect(operation.getSnapshot()).toMatchObject({
    storageWarning: true,
    busy: false,
    pending: null,
  });
  storage.mockRestore();
  const reloaded = new ThemeOperation("full");
  expect(reloaded.getSnapshot().pending).toBeNull();
  mutate.mockResolvedValue(undefined);
  await reloaded.run({ method: "DELETE", path: "/themes/custom" });
  expect(mutate).toHaveBeenCalledTimes(1);
});

it("recovers the exact completed save after renewal without replay or desired-state authority", async () => {
  const source = {
    source: '{"id":"custom"}',
    assets: {},
    sourceDigest: "a".repeat(64),
    diagnostics: [],
  };
  const mutate = vi
    .spyOn(themeAPI, "mutate")
    .mockRejectedValue(new TypeError("lost response"));
  const read = vi.spyOn(themeAPI, "source").mockResolvedValue(source);
  const outcome = vi
    .spyOn(apiClient, "apiFetch")
    .mockResolvedValue({
      state: "completed",
      response_status: 200,
      response_body: source,
    });
  const operation = new ThemeOperation("binding");
  await expect(
    operation.run({
      method: "PUT",
      path: "/themes/custom/source",
      body: { source: source.source, assets: {}, expected_source_digest: "" },
    }),
  ).rejects.toThrow();
  const original = operation.getSnapshot().pending!;
  setCSRFToken("new-csrf", "renewed-session");
  await expect(operation.retry()).resolves.toEqual({
    request: original.request,
    result: source,
  });
  expect(mutate).toHaveBeenCalledTimes(1);
  expect(read).not.toHaveBeenCalled();
  expect(outcome).toHaveBeenCalledWith(
    `/mutation-outcomes/${encodeURIComponent(original.key)}`,
    { params: { operation: "theme-source-save" } },
  );
  expect(operation.getSnapshot().pending).toBeNull();
  expect(new ThemeOperation("binding").getSnapshot().pending).toBeNull();
});
it("retains unmatched cross-binding outcomes and never sends a new key or draft", async () => {
  const mutate = vi
    .spyOn(themeAPI, "mutate")
    .mockRejectedValue(new TypeError("lost response"));
  vi.spyOn(apiClient, "apiFetch").mockResolvedValue({ state: "unknown" });
  vi.spyOn(themeAPI, "source").mockResolvedValue({
    source: '{"id":"custom","name":"another edit"}',
    assets: {},
    sourceDigest: "a".repeat(64),
    diagnostics: [],
  });
  const operation = new ThemeOperation("unmatched");
  await expect(
    operation.run({
      method: "PUT",
      path: "/themes/custom/source",
      body: {
        source: '{"id":"custom"}',
        assets: {},
        expected_source_digest: "",
      },
    }),
  ).rejects.toThrow();
  const pending = operation.getSnapshot().pending;
  setCSRFToken("new-csrf", "renewed-session");
  await expect(operation.retry()).rejects.toThrow(/unresolved/);
  expect(mutate).toHaveBeenCalledTimes(1);
  expect(operation.getSnapshot().pending).toEqual(pending);
  await expect(
    operation.run({ method: "DELETE", path: "/themes/custom" }),
  ).rejects.toThrow(/Recover/);
});

it.each([
  {
    operation: "theme-source-save",
    request: {
      method: "PUT",
      path: "/themes/custom/source",
      body: {
        source: '{"id":"custom"}',
        assets: {},
        expected_source_digest: "",
      },
    },
    status: 200,
  },
  {
    operation: "theme-source-delete",
    request: {
      method: "DELETE",
      path: "/themes/custom/source",
      body: { expected_source_digest: "a".repeat(64) },
    },
    status: 204,
  },
  {
    operation: "theme-publish",
    request: {
      method: "POST",
      path: "/themes/custom/publish",
      body: { expected_source_digest: "a".repeat(64) },
      verify: { id: "custom", resolvedDigest: "b".repeat(64) },
    },
    status: 201,
  },
  {
    operation: "theme-unpublish",
    request: { method: "DELETE", path: "/themes/custom" },
    status: 204,
  },
  {
    operation: "theme-import",
    request: {
      method: "POST",
      path: "/themes/import",
      body: { archive: "cGs=" },
      verify: { id: "custom", resolvedDigest: "b".repeat(64) },
    },
    status: 201,
  },
])(
  "reads the exact $operation outcome after reload and renewal",
  async ({ operation: kind, request, status }) => {
    const mutate = vi
      .spyOn(themeAPI, "mutate")
      .mockRejectedValue(new TypeError("lost response"));
    const body = status === 204 ? undefined : { id: "original-result" };
    const outcome = vi
      .spyOn(apiClient, "apiFetch")
      .mockResolvedValue({
        state: "completed",
        response_status: status,
        response_body: body,
      });
    const original = new ThemeOperation(kind);
    await expect(original.run(request as SharedThemeRequest)).rejects.toThrow(
      "lost response",
    );
    const pending = original.getSnapshot().pending!;
    setCSRFToken("new-csrf", "renewed-session");
    const restored = new ThemeOperation(kind);
    await expect(restored.retry()).resolves.toEqual({
      request: pending.request,
      result: body,
    });
    expect(outcome).toHaveBeenCalledWith(
      `/mutation-outcomes/${encodeURIComponent(pending.key)}`,
      { params: { operation: kind } },
    );
    expect(mutate).toHaveBeenCalledTimes(1);
    expect(restored.getSnapshot().pending).toBeNull();
    expect(new ThemeOperation(kind).getSnapshot().pending).toBeNull();
  },
);

it.each([
  { state: "unknown" },
  { state: "pending" },
  { state: "completed" },
  { state: "completed", response_status: 200 },
])(
  "keeps incomplete outcome $state/$response_status unresolved",
  async (outcome) => {
    const mutate = vi
      .spyOn(themeAPI, "mutate")
      .mockRejectedValue(new TypeError("lost response"));
    vi.spyOn(apiClient, "apiFetch").mockResolvedValue(outcome);
    const operation = new ThemeOperation("unresolved");
    await expect(
      operation.run({ method: "DELETE", path: "/themes/custom" }),
    ).rejects.toThrow();
    const pending = operation.getSnapshot().pending;
    setCSRFToken("new-csrf", "renewed-session");
    await expect(operation.retry()).rejects.toThrow(/unresolved/);
    expect(operation.getSnapshot().pending).toEqual(pending);
    expect(mutate).toHaveBeenCalledTimes(1);
  },
);

it("clears a definitively rejected original change while preserving its canonical error", async () => {
  const mutate = vi
    .spyOn(themeAPI, "mutate")
    .mockRejectedValue(new TypeError("lost response"));
  vi.spyOn(apiClient, "apiFetch").mockResolvedValue({
    state: "completed",
    response_status: 409,
    response_body: {
      error: {
        message: "Source changed",
        code: "dependency_blocked",
        category: "core",
        retryable: false,
        details: { expected: "original" },
        correlation_id: "original-request",
      },
    },
  });
  const operation = new ThemeOperation("rejected");
  await expect(
    operation.run({ method: "DELETE", path: "/themes/custom" }),
  ).rejects.toThrow();
  setCSRFToken("new-csrf", "renewed-session");
  await expect(operation.retry()).rejects.toMatchObject({
    status: 409,
    code: "dependency_blocked",
    correlationId: "original-request",
    details: { expected: "original" },
  });
  expect(operation.getSnapshot().pending).toBeNull();
  expect(new ThemeOperation("rejected").getSnapshot().pending).toBeNull();
  expect(mutate).toHaveBeenCalledTimes(1);
});

it("keeps the original request when outcome readback itself fails authentication", async () => {
  const mutate = vi
    .spyOn(themeAPI, "mutate")
    .mockRejectedValue(new TypeError("lost response"));
  vi.spyOn(apiClient, "apiFetch").mockRejectedValue(
    new ApiError(401, {
      message: "Session expired",
      code: "policy_violation",
      category: "core",
      retryable: false,
      details: {},
      correlationId: "auth",
    }),
  );
  const operation = new ThemeOperation("expired-read");
  await expect(
    operation.run({ method: "DELETE", path: "/themes/custom" }),
  ).rejects.toThrow();
  const pending = operation.getSnapshot().pending;
  setCSRFToken("new-csrf", "renewed-session");
  await expect(operation.retry()).rejects.toThrow("Session expired");
  expect(operation.getSnapshot().pending).toEqual(pending);
  expect(mutate).toHaveBeenCalledTimes(1);
});

it("rejects malformed stored bodies and preserves a blocking recovery marker", async () => {
  const record = {
    key: newIdempotencyKey("test"),
    origin: window.location.origin,
    authBinding: "first-session",
  };
  for (const request of [
    { method: "PUT", path: "/themes/custom/source" },
    {
      method: "PUT",
      path: "/themes/custom/source",
      body: {
        source: '{"id":"custom"}',
        assets: [],
        expected_source_digest: "",
      },
    },
    {
      method: "PUT",
      path: "/themes/custom/source",
      body: {
        source: '{"id":"custom"}',
        assets: { "../outside.svg": "cGs=" },
        expected_source_digest: "",
      },
    },
    {
      method: "POST",
      path: "/themes/custom/publish",
      body: { expected_source_digest: "" },
    },
    { method: "DELETE", path: "/themes/custom", body: { arbitrary: true } },
  ]) {
    sessionStorage.setItem(
      "kingdom:theme-operation:v1:invalid",
      JSON.stringify({ ...record, request }),
    );
    const operation = new ThemeOperation("invalid");
    expect(operation.getSnapshot()).toMatchObject({
      pending: null,
      blockedRecovery: true,
    });
    await expect(
      operation.run({ method: "DELETE", path: "/themes/custom" }),
    ).rejects.toThrow(/invalid recovery/);
  }
});
it("keeps expired-key rejection unresolved after a previous unknown outcome", async () => {
  vi.spyOn(themeAPI, "mutate")
    .mockRejectedValueOnce(new TypeError("offline"))
    .mockRejectedValue(
      new ApiError(409, {
        message: "Key expired",
        code: "idempotency_key_expired",
        category: "core",
        retryable: false,
        details: {},
        correlationId: "idempotency-middleware",
      }),
    );
  const operation = new ThemeOperation("expired");
  await expect(
    operation.run({ method: "DELETE", path: "/themes/custom" }),
  ).rejects.toThrow("offline");
  const pending = operation.getSnapshot().pending;
  await expect(operation.retry()).rejects.toThrow("Key expired");
  expect(operation.getSnapshot().pending).toEqual(pending);
});
