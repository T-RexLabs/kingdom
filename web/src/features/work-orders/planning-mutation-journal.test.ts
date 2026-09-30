import { beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, setCSRFToken } from "@/api/client";
import { PlanningMutationJournal } from "./planning-mutation-journal";

beforeEach(() => { sessionStorage.clear(); setCSRFToken("token", "binding-a"); vi.unstubAllGlobals(); });
describe("ordinary planning mutation journal", () => {
  it("persists before send and recovers the exact request after a reload", async () => {
    const journal = new PlanningMutationJournal();
    const send = vi.fn(async () => { expect(new PlanningMutationJournal().get("title")?.body).toEqual({ title: "Original" }); throw new Error("lost response"); });
    await expect(journal.execute("title", { title: "Original" }, "key-original", send)).rejects.toThrow();
    const retry = vi.fn(async () => ({ title: "Original" }));
    await new PlanningMutationJournal().execute("title", { title: "Later edit" }, "key-new", retry);
    expect(retry).toHaveBeenCalledWith({ title: "Original" }, "key-original");
    expect(new PlanningMutationJournal().get("title")).toBeNull();
  });
  it("keeps independent sessions and slices isolated", async () => {
    const journal = new PlanningMutationJournal();
    await expect(journal.execute("A:title", { title: "A" }, "A", async () => { throw new Error("lost"); })).rejects.toThrow();
    await journal.execute("B:title", { title: "B" }, "B", async () => ({}));
    await journal.execute("A:model", { model: "model" }, "M", async () => ({}));
    expect(journal.get("A:title")?.key).toBe("A");
  });
  it("blocks replacement-binding replay of an unresolved ordinary update", async () => {
    const journal = new PlanningMutationJournal();
    await expect(journal.execute("title", { title: "A" }, "key", async () => { throw new Error("lost"); })).rejects.toThrow();
    setCSRFToken("replacement", "binding-b");
    const send = vi.fn();
    await expect(new PlanningMutationJournal().execute("title", { title: "B" }, "new", send)).rejects.toThrow("earlier browser session");
    expect(send).not.toHaveBeenCalled();
    expect(new PlanningMutationJournal().get("title")?.key).toBe("key");
  });
  it.each(["unknown", "pending"])("does not fresh-send a %s creation outcome", async (state) => {
    const journal = new PlanningMutationJournal();
    await expect(journal.execute("create", {}, "key", async () => { throw new Error("lost"); })).rejects.toThrow();
    setCSRFToken("replacement", "binding-b");
    const send = vi.fn();
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ state }), { status: 200 })));
    await expect(new PlanningMutationJournal().execute("create", {}, "new", send, "planning-session-create")).rejects.toThrow(state);
    expect(send).not.toHaveBeenCalled();
    expect(new PlanningMutationJournal().get("create")?.key).toBe("key");
  });
  it.each([
    { status: 401, code: "unknown", correlationId: "api.auth" },
    { status: 403, code: "policy_violation", correlationId: "api.auth.csrf" },
    { status: 409, code: "idempotency_conflict", correlationId: "idempotency-middleware" },
    { status: 409, code: "idempotency_key_expired", correlationId: "idempotency-middleware" },
  ] as const)("keeps an unknown original request after a replay boundary rejects with $code/$status", async (rejection) => {
    const journal = new PlanningMutationJournal();
    await expect(journal.execute("title", { title: "Original" }, "original-key", async () => { throw new TypeError("response lost"); })).rejects.toThrow();
    const retry = vi.fn(async () => { throw new ApiError(rejection.status, { message: "Replay rejected", code: rejection.code, correlationId: rejection.correlationId, category: "core", retryable: false, details: {} }); });
    await expect(new PlanningMutationJournal().execute("title", { title: "Later" }, "replacement-key", retry)).rejects.toThrow("Replay rejected");
    expect(retry).toHaveBeenCalledWith({ title: "Original" }, "original-key");
    expect(new PlanningMutationJournal().get("title")).toMatchObject({ key: "original-key", body: { title: "Original" } });
  });
  it("reconciles an authoritative creation response without cross-binding mutation", async () => {
    const journal = new PlanningMutationJournal();
    await expect(journal.execute("create", {}, "key", async () => { throw new Error("lost"); })).rejects.toThrow();
    setCSRFToken("replacement", "binding-b");
    const send = vi.fn();
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ state: "completed", response_status: 201, response_body: { session: { planning_session_id: "created" } } }), { status: 200 })));
    await expect(new PlanningMutationJournal().execute("create", {}, "new", send, "planning-session-create")).resolves.toEqual({ session: { planning_session_id: "created" } });
    expect(send).not.toHaveBeenCalled();
    expect(new PlanningMutationJournal().get("create")).toBeNull();
  });

  it("keeps the original request when an outcome lookup itself is rejected", async () => {
    const journal = new PlanningMutationJournal();
    await expect(journal.execute("title", { title: "Original" }, "original-key", async () => { throw new TypeError("lost"); })).rejects.toThrow();
    setCSRFToken("renewed", "renewed-binding");
    const send = vi.fn();
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({ error: { code: "policy_violation", message: "Lookup denied", category: "core", details: {}, retryable: false, correlation_id: "outcome-lookup" } }), { status: 403 })));
    await expect(new PlanningMutationJournal().execute("title", { title: "Different" }, "different-key", send, "planning-session-create")).rejects.toThrow("Lookup denied");
    expect(send).not.toHaveBeenCalled();
    expect(new PlanningMutationJournal().get("title")?.key).toBe("original-key");
  });

  it("selects the outcome operation from the retained request and releases a definitive original rejection", async () => {
    type SourceRequest = { action: "adopt" | "detach"; link: string };
    const original: SourceRequest = { action: "adopt", link: "original-link" };
    await expect(new PlanningMutationJournal().execute("source", original, "original-key", async () => { throw new TypeError("lost"); })).rejects.toThrow();
    setCSRFToken("renewed", "renewed-binding");
    const fetch = vi.fn(async () => new Response(JSON.stringify({ state: "completed", response_status: 409, response_body: { error: { code: "policy_violation", message: "Original adoption rejected", category: "core", details: {}, correlation_id: "original-response", retryable: false } } }), { status: 200 }));
    vi.stubGlobal("fetch", fetch);
    const send = vi.fn();
    await expect(new PlanningMutationJournal().execute<SourceRequest, unknown>("source", { action: "detach", link: "different-link" }, "replacement-key", send, (request) => request.action === "adopt" ? "external-source-adopt" : "external-source-detach")).rejects.toThrow("Original adoption rejected");
    expect(fetch).toHaveBeenCalledWith("/api/v1/mutation-outcomes/original-key?operation=external-source-adopt", expect.objectContaining({ credentials: "same-origin" }));
    expect(send).not.toHaveBeenCalled();
    expect(new PlanningMutationJournal().get("source")).toBeNull();
  });

  it.each([
    { state: "completed", response_status: 200 },
    { state: "completed", response_status: 302, response_body: {} },
  ])("does not discard the guard for an unverifiable completed outcome: %j", async (outcome) => {
    await expect(new PlanningMutationJournal().execute("create", {}, "original-key", async () => { throw new TypeError("lost"); })).rejects.toThrow();
    setCSRFToken("renewed", "renewed-binding");
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(outcome), { status: 200 })));
    const send = vi.fn();
    await expect(new PlanningMutationJournal().execute("create", {}, "replacement-key", send, "planning-session-create")).rejects.toThrow();
    expect(send).not.toHaveBeenCalled();
    expect(new PlanningMutationJournal().get("create")?.key).toBe("original-key");
  });
});
