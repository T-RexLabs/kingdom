import { beforeEach, expect, it, vi } from "vitest";
import { ApiError, setCSRFToken } from "@/api/client";
import { newIdempotencyKey } from "@/lib/idempotency";
import { themeAPI } from "./theme-api";
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

it("never replays a pending change with a renewed browser binding", async () => {
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
  const operation = new ThemeOperation("binding");
  await expect(
    operation.run({
      method: "PUT",
      path: "/themes/custom/source",
      body: { source: source.source, assets: {}, expected_source_digest: "" },
    }),
  ).rejects.toThrow();
  setCSRFToken("new-csrf", "renewed-session");
  await expect(operation.retry()).rejects.toThrow(
    /current shared state matches.*unresolved/,
  );
  expect(mutate).toHaveBeenCalledTimes(1);
  expect(read).toHaveBeenCalledWith("custom");
  expect(operation.getSnapshot().pending).not.toBeNull();
});
it("retains unmatched cross-binding outcomes and never sends a new key or draft", async () => {
  const mutate = vi
    .spyOn(themeAPI, "mutate")
    .mockRejectedValue(new TypeError("lost response"));
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
